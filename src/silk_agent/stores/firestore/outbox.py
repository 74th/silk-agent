"""Atomic creation of sanitized, epoch-fenced output records."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional
from google.cloud.firestore_v1.base_query import FieldFilter

from silk_agent.core.distributed_models import (
    OutputDeliveryState,
    OwnershipToken,
    validate_state_transition,
)
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import conversation_document_id, document_id
from silk_agent.stores.firestore.ownership import ownership_matches
from silk_agent.stores.firestore.retention import RetentionPolicy, output_expiry


class FirestoreOutboxWriter:
    """Persist one output only while the writer still owns an active conversation."""

    def __init__(
        self,
        client: firestore.AsyncClient,
        *,
        collection_prefix: str = "silk-agent",
        retention: RetentionPolicy | None = None,
    ):
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("collection_prefix must be non-empty and cannot contain '/'")
        self.client = client
        self.collection_prefix = collection_prefix
        self.retention = retention or RetentionPolicy()

    def _retention_patch(
        self, previous: Mapping[str, Any], state: OutputDeliveryState, now: datetime
    ) -> dict[str, Any]:
        """`expire_at` for a delivered / suppressed output; removed if it goes back."""
        expiry = output_expiry(state, now, retention=self.retention.terminal)
        if expiry is not None:
            return {"expire_at": expiry}
        if previous.get("expire_at") is not None:
            return {"expire_at": firestore.DELETE_FIELD}
        return {}

    async def put_sanitized(
        self,
        *,
        output_id: str,
        conversation_key: str,
        conversation_generation: int,
        sequence: int,
        origin_seq: int | None = None,
        owner: OwnershipToken,
        payload: Mapping[str, Any],
        now: datetime,
        created_at: datetime,
    ) -> bool:
        if not output_id or not isinstance(payload, Mapping):
            raise ValueError("output_id and mapping payload are required")
        _validate_utc(now, "now")
        _validate_utc(created_at, "created_at")
        if (
            owner.conversation_key != conversation_key
            or owner.conversation_generation != conversation_generation
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence < 1
            or (
                origin_seq is not None
                and (
                    not isinstance(origin_seq, int)
                    or isinstance(origin_seq, bool)
                    or origin_seq < 1
                )
            )
        ):
            raise ValueError("output conversation, generation, or sequence is invalid")

        conversation_id = conversation_document_id(conversation_key)
        conversation_ref = self.client.collection(
            f"{self.collection_prefix}_conversations"
        ).document(conversation_id)
        owner_ref = self.client.collection(f"{self.collection_prefix}_ownership").document(
            conversation_id
        )
        output_ref = self.client.collection(f"{self.collection_prefix}_outbox").document(
            document_id(output_id, prefix="output")
        )
        record = {
            "output_id": output_id,
            "conversation_key": conversation_key,
            "conversation_generation": conversation_generation,
            "sequence": sequence,
            "origin_seq": origin_seq,
            "epoch": owner.epoch,
            "kind": str(payload.get("kind", "message")),
            "payload": dict(payload),
            "status": "pending",
            "attempt_id": None,
            "next_attempt_at": created_at.astimezone(UTC).isoformat(),
            "created_at": created_at.astimezone(UTC).isoformat(),
            "schema_version": 1,
        }
        transaction = self.client.transaction()

        @async_transactional
        async def write(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            output_snapshot = await output_ref.get(transaction=transaction)
            if not owner_snapshot.exists or not conversation_snapshot.exists:
                return False
            conversation = conversation_snapshot.to_dict() or {}
            if conversation.get("reset_pending") or not ownership_matches(
                owner_snapshot.to_dict() or {},
                conversation,
                owner,
                now=now,
            ):
                return False
            stop_seq = conversation.get("stop_seq", 0)
            if (
                origin_seq is not None
                and isinstance(stop_seq, int)
                and not isinstance(stop_seq, bool)
                and origin_seq <= stop_seq
            ):
                return False
            if output_snapshot.exists:
                return (output_snapshot.to_dict() or {}) == record
            transaction.create(output_ref, record)
            return True

        try:
            return await write(transaction)
        except Exception as exc:
            raise convert_firestore_error("put_sanitized_outbox", exc) from None


class FirestoreOutboxStore(FirestoreOutboxWriter):
    """Conversation-ordered output claims for independent Slack Sender processes."""

    async def list_conversations_with_pending(self, *, limit: int = 100) -> tuple[str, ...]:
        """Bounded page of conversations that have output waiting for a Sender."""
        if limit < 1:
            raise ValueError("limit must be positive")
        query = (
            self.client.collection(f"{self.collection_prefix}_outbox")
            .where(
                filter=FieldFilter(
                    "status",
                    "in",
                    [OutputDeliveryState.PENDING.value, OutputDeliveryState.DISPATCHING.value],
                )
            )
            .limit(limit)
        )
        try:
            keys: dict[str, None] = {}
            async for snapshot in query.stream():
                key = (snapshot.to_dict() or {}).get("conversation_key")
                if isinstance(key, str) and key:
                    keys[key] = None
            return tuple(keys)
        except Exception as exc:
            raise convert_firestore_error("list_outbox_conversations", exc) from None

    async def claim_next(
        self,
        *,
        conversation_key: str,
        sender_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> Mapping[str, Any] | None:
        _validate_utc(now, "now")
        _validate_utc(lease_expires_at, "lease_expires_at")
        if not sender_id or lease_expires_at <= now:
            raise ValueError("sender_id and future lease expiry are required")
        query = (
            self.client.collection(f"{self.collection_prefix}_outbox")
            .where(filter=FieldFilter("conversation_key", "==", conversation_key))
            .where(
                filter=FieldFilter(
                    "status",
                    "in",
                    [
                        OutputDeliveryState.PENDING.value,
                        OutputDeliveryState.DISPATCHING.value,
                        OutputDeliveryState.UNCERTAIN.value,
                    ],
                )
            )
            .order_by("sequence")
            .limit(100)
        )
        transaction = self.client.transaction()
        attempt_id = uuid4().hex

        @async_transactional
        async def claim(transaction: firestore.AsyncTransaction) -> Mapping[str, Any] | None:
            query_result = await transaction.get(query)
            snapshots = [snapshot async for snapshot in query_result]
            for snapshot in snapshots:
                data = snapshot.to_dict() or {}
                state = data.get("status")
                if state == OutputDeliveryState.UNCERTAIN.value:
                    return None
                next_attempt_at = data.get("next_attempt_at")
                if isinstance(next_attempt_at, str):
                    try:
                        next_attempt_at = datetime.fromisoformat(next_attempt_at)
                    except ValueError:
                        return None
                if isinstance(next_attempt_at, datetime) and next_attempt_at > now:
                    return None
                if state == OutputDeliveryState.DISPATCHING.value:
                    expiry = data.get("lease_expires_at")
                    if isinstance(expiry, datetime) and expiry <= now:
                        transaction.update(
                            snapshot.reference,
                            {
                                "status": OutputDeliveryState.UNCERTAIN.value,
                                "updated_at": now,
                                "lease_expires_at": firestore.DELETE_FIELD,
                            },
                        )
                    return None
                if state != OutputDeliveryState.PENDING.value:
                    continue
                transaction.update(
                    snapshot.reference,
                    {
                        "status": OutputDeliveryState.DISPATCHING.value,
                        "sender_id": sender_id,
                        "attempt_id": attempt_id,
                        "lease_expires_at": lease_expires_at,
                        "attempt_count": (data.get("attempt_count", 0) or 0) + 1,
                        "updated_at": now,
                    },
                )
                return {
                    **data,
                    "status": OutputDeliveryState.DISPATCHING.value,
                    "attempt_id": attempt_id,
                }
            return None

        try:
            return await claim(transaction)
        except Exception as exc:
            raise convert_firestore_error("claim_outbox_output", exc) from None

    async def mark_delivery(
        self,
        *,
        output_id: str,
        expected: OutputDeliveryState,
        new_state: OutputDeliveryState,
        attempt_id: str,
        slack_ref: Mapping[str, str] | None = None,
        next_attempt_at: datetime | None = None,
        updated_at: datetime,
    ) -> bool:
        _validate_utc(updated_at, "updated_at")
        if next_attempt_at is not None:
            _validate_utc(next_attempt_at, "next_attempt_at")
        validate_state_transition(expected, new_state)
        ref = self.client.collection(f"{self.collection_prefix}_outbox").document(
            document_id(output_id, prefix="output")
        )
        transaction = self.client.transaction()

        @async_transactional
        async def update(transaction: firestore.AsyncTransaction) -> bool:
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return False
            data = snapshot.to_dict() or {}
            if data.get("status") != expected.value or data.get("attempt_id") != attempt_id:
                return False
            patch: dict[str, Any] = {
                "status": new_state.value,
                "updated_at": updated_at,
                "lease_expires_at": firestore.DELETE_FIELD,
                "sender_id": firestore.DELETE_FIELD,
                **self._retention_patch(data, new_state, updated_at),
            }
            if slack_ref is not None:
                patch["slack_ref"] = dict(slack_ref)
            if next_attempt_at is not None:
                patch["next_attempt_at"] = next_attempt_at
            transaction.update(ref, patch)
            return True

        try:
            return await update(transaction)
        except Exception as exc:
            raise convert_firestore_error("mark_outbox_delivery", exc) from None

    async def revalidate_claim(self, *, output_id: str, attempt_id: str, now: datetime) -> bool:
        _validate_utc(now, "now")
        ref = self.client.collection(f"{self.collection_prefix}_outbox").document(
            document_id(output_id, prefix="output")
        )
        transaction = self.client.transaction()

        @async_transactional
        async def validate(transaction: firestore.AsyncTransaction) -> bool:
            output_snapshot = await ref.get(transaction=transaction)
            if not output_snapshot.exists:
                return False
            data = output_snapshot.to_dict() or {}
            if (
                data.get("status") != OutputDeliveryState.DISPATCHING.value
                or data.get("attempt_id") != attempt_id
            ):
                return False
            conversation_key = data.get("conversation_key")
            if not isinstance(conversation_key, str) or not conversation_key:
                transaction.update(
                    ref,
                    {
                        "status": OutputDeliveryState.SUPPRESSED.value,
                        **self._retention_patch(data, OutputDeliveryState.SUPPRESSED, now),
                    },
                )
                return False
            conversation_ref = self.client.collection(
                f"{self.collection_prefix}_conversations"
            ).document(conversation_document_id(conversation_key))
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            conversation = (
                (conversation_snapshot.to_dict() or {}) if conversation_snapshot.exists else {}
            )
            origin_seq = data.get("origin_seq")
            stop_seq = conversation.get("stop_seq", 0) if conversation else 0
            stale = (
                not conversation_snapshot.exists
                or conversation.get("conversation_generation")
                != data.get("conversation_generation")
                or conversation.get("reset_pending") is True
                or (
                    isinstance(origin_seq, int)
                    and not isinstance(origin_seq, bool)
                    and isinstance(stop_seq, int)
                    and not isinstance(stop_seq, bool)
                    and origin_seq <= stop_seq
                )
            )
            if stale:
                transaction.update(
                    ref,
                    {
                        "status": OutputDeliveryState.SUPPRESSED.value,
                        "updated_at": now,
                        "lease_expires_at": firestore.DELETE_FIELD,
                        "sender_id": firestore.DELETE_FIELD,
                        **self._retention_patch(data, OutputDeliveryState.SUPPRESSED, now),
                    },
                )
                return False
            return True

        try:
            return await validate(transaction)
        except Exception as exc:
            raise convert_firestore_error("revalidate_outbox_claim", exc) from None


def _validate_utc(value: datetime, field_name: str) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
