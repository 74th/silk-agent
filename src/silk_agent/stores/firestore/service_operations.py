"""Ownership-fenced acceptance ledger for Sandbox Service mutations."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional
from google.cloud.firestore_v1.base_query import FieldFilter

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.distributed_ports import ServiceOperationStore
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import conversation_document_id, document_id
from silk_agent.stores.firestore.ownership import ownership_matches


class FirestoreServiceOperationStore:
    """Atomically persist accepted operation intent before remote mutation is attempted."""

    def __init__(self, client: firestore.AsyncClient, *, collection_prefix: str = "silk-agent"):
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("collection_prefix must be non-empty and cannot contain '/'")
        self.client = client
        self.collection_prefix = collection_prefix

    async def accept_intent(
        self,
        *,
        operation_id: str,
        operation_kind: str,
        request_hash: str,
        conversation_key: str,
        conversation_generation: int,
        sandbox_generation: int,
        sandbox_key_hash: str | None = None,
        turn_id: str,
        origin_seq: int,
        owner: OwnershipToken,
        payload: Mapping[str, Any],
        now: datetime,
        created_at: datetime,
    ) -> bool:
        if not all((operation_id, operation_kind, request_hash, conversation_key, turn_id)):
            raise ValueError(
                "operation ID, kind, request hash, conversation, and turn are required"
            )
        _validate_utc(now, "now")
        _validate_utc(created_at, "created_at")
        if (
            owner.conversation_key != conversation_key
            or owner.conversation_generation != conversation_generation
            or not isinstance(origin_seq, int)
            or isinstance(origin_seq, bool)
            or origin_seq < 1
            or not isinstance(sandbox_generation, int)
            or isinstance(sandbox_generation, bool)
            or sandbox_generation < 1
        ):
            raise ValueError("operation generations, sequence, or owner are invalid")
        if sandbox_key_hash is not None and re.fullmatch(r"[0-9a-f]{64}", sandbox_key_hash) is None:
            raise ValueError("sandbox_key_hash must be a lowercase SHA-256 hash")

        conversation_id = conversation_document_id(conversation_key)
        conversation_ref = self.client.collection(
            f"{self.collection_prefix}_conversations"
        ).document(conversation_id)
        owner_ref = self.client.collection(f"{self.collection_prefix}_ownership").document(
            conversation_id
        )
        sandbox_ref = self.client.collection(f"{self.collection_prefix}_sandboxes").document(
            sandbox_key_hash or conversation_id
        )
        operation_ref = self.client.collection(
            f"{self.collection_prefix}_sandbox_operations"
        ).document(document_id(operation_id, prefix="operation"))
        command_ref = self.client.collection(f"{self.collection_prefix}_commands").document(
            document_id(operation_id, prefix="command")
        )
        record = {
            "operation_id": operation_id,
            "operation_kind": operation_kind,
            "request_hash": request_hash,
            "conversation_key": conversation_key,
            "conversation_generation": conversation_generation,
            "sandbox_generation": sandbox_generation,
            "sandbox_key_hash": sandbox_key_hash,
            "turn_id": turn_id,
            "origin_seq": origin_seq,
            "origin_epoch": owner.epoch,
            "worker_incarnation": owner.worker_incarnation,
            "payload": dict(payload),
            "state": "accepted",
            "created_at": created_at.astimezone(UTC).isoformat(),
            "schema_version": 1,
        }
        transaction = self.client.transaction()

        @async_transactional
        async def accept(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            sandbox_snapshot = await sandbox_ref.get(transaction=transaction)
            operation_snapshot = await operation_ref.get(transaction=transaction)
            command_snapshot = (
                await command_ref.get(transaction=transaction)
                if operation_kind == "command_start"
                else None
            )
            if not owner_snapshot.exists or not conversation_snapshot.exists:
                return False
            owner_data = owner_snapshot.to_dict() or {}
            conversation_data = conversation_snapshot.to_dict() or {}
            if not ownership_matches(
                owner_data,
                conversation_data,
                owner,
                now=now,
            ):
                return False
            stop_seq = conversation_data.get("stop_seq", 0)
            if (
                not isinstance(stop_seq, int)
                or isinstance(stop_seq, bool)
                or origin_seq <= stop_seq
                or not sandbox_snapshot.exists
            ):
                return False
            sandbox_data = sandbox_snapshot.to_dict() or {}
            if sandbox_key_hash is None:
                if (
                    sandbox_data.get("conversation_generation") != conversation_generation
                    or sandbox_data.get("sandbox_generation") != sandbox_generation
                ):
                    return False
            elif (
                sandbox_data.get("key_hash") != sandbox_key_hash
                or sandbox_data.get("generation") != sandbox_generation
                or sandbox_data.get("state") != "ready"
            ):
                return False
            if operation_snapshot.exists:
                return False
            if command_snapshot is not None and command_snapshot.exists:
                return False
            transaction.create(operation_ref, record)
            if operation_kind == "command_start":
                # The command monitor ledger must exist before the Service dispatches
                # tracker.start. The request ID is also the stable command ID.
                transaction.create(
                    command_ref,
                    {
                        "command_id": operation_id,
                        "request_hash": request_hash,
                        "conversation_key": conversation_key,
                        "conversation_generation": conversation_generation,
                        "sandbox_generation": sandbox_generation,
                        "origin_turn": turn_id,
                        "origin_epoch": owner.epoch,
                        "origin_seq": origin_seq,
                        "worker_incarnation": owner.worker_incarnation,
                        "state": "intent",
                        "delivery_mode": "foreground",
                        "notification_state": "none",
                        "created_at": created_at,
                        "updated_at": now,
                        "next_attempt_at": now,
                        "schema_version": 1,
                    },
                )
            return True

        try:
            return await accept(transaction)
        except Exception as exc:
            raise convert_firestore_error("accept_sandbox_operation", exc) from None

    async def get_operation(self, operation_id: str) -> Mapping[str, Any] | None:
        ref = self.client.collection(f"{self.collection_prefix}_sandbox_operations").document(
            document_id(operation_id, prefix="operation")
        )
        try:
            snapshot = await ref.get()
            return snapshot.to_dict() if snapshot.exists else None
        except Exception as exc:
            raise convert_firestore_error("get_sandbox_operation", exc) from None

    async def is_dispatch_allowed(
        self, operation_id: str, owner: OwnershipToken, *, now: datetime
    ) -> bool:
        _validate_utc(now, "now")
        operation_ref = self.client.collection(
            f"{self.collection_prefix}_sandbox_operations"
        ).document(document_id(operation_id, prefix="operation"))
        transaction = self.client.transaction(read_only=True)

        @async_transactional
        async def check(transaction: firestore.AsyncTransaction) -> bool:
            operation_snapshot = await operation_ref.get(transaction=transaction)
            if not operation_snapshot.exists:
                return False
            operation = operation_snapshot.to_dict() or {}
            conversation_key = operation.get("conversation_key")
            if not isinstance(conversation_key, str) or conversation_key != owner.conversation_key:
                return False
            conversation_id = conversation_document_id(conversation_key)
            owner_ref = self.client.collection(f"{self.collection_prefix}_ownership").document(
                conversation_id
            )
            conversation_ref = self.client.collection(
                f"{self.collection_prefix}_conversations"
            ).document(conversation_id)
            sandbox_ref = self.client.collection(f"{self.collection_prefix}_sandboxes").document(
                operation.get("sandbox_key_hash") or conversation_id
            )
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            sandbox_snapshot = await sandbox_ref.get(transaction=transaction)
            if (
                not owner_snapshot.exists
                or not conversation_snapshot.exists
                or not sandbox_snapshot.exists
            ):
                return False
            owner_data = owner_snapshot.to_dict() or {}
            conversation_data = conversation_snapshot.to_dict() or {}
            sandbox_data = sandbox_snapshot.to_dict() or {}
            if not ownership_matches(owner_data, conversation_data, owner, now=now):
                return False
            stop_seq = conversation_data.get("stop_seq", 0)
            if (
                operation.get("state") != "accepted"
                or operation.get("origin_epoch") != owner.epoch
                or operation.get("conversation_generation") != owner.conversation_generation
                or not isinstance(stop_seq, int)
                or isinstance(stop_seq, bool)
                or operation.get("origin_seq", 0) <= stop_seq
            ):
                return False
            sandbox_key_hash = operation.get("sandbox_key_hash")
            if sandbox_key_hash:
                return (
                    sandbox_data.get("key_hash") == sandbox_key_hash
                    and sandbox_data.get("generation") == operation.get("sandbox_generation")
                    and sandbox_data.get("state") == "ready"
                )
            return sandbox_data.get("conversation_generation") == operation.get(
                "conversation_generation"
            ) and sandbox_data.get("sandbox_generation") == operation.get("sandbox_generation")

        try:
            return await check(transaction)
        except Exception as exc:
            raise convert_firestore_error("check_sandbox_operation_dispatch", exc) from None

    async def list_for_stop(
        self,
        *,
        conversation_key: str,
        conversation_generation: int,
        stop_seq: int,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[tuple[Mapping[str, Any], ...], str | None]:
        if not conversation_key or conversation_generation < 1 or stop_seq < 0 or limit < 1:
            raise ValueError("invalid stop operation query")
        collection = self.client.collection(f"{self.collection_prefix}_sandbox_operations")
        query = collection.where(filter=FieldFilter("conversation_key", "==", conversation_key))
        query = query.where(
            filter=FieldFilter("conversation_generation", "==", conversation_generation)
        )
        query = query.where(filter=FieldFilter("state", "==", "accepted"))
        query = query.where(filter=FieldFilter("origin_seq", "<=", stop_seq))
        query = query.order_by("origin_seq").order_by("__name__").limit(limit + 1)
        if cursor is not None:
            cursor_ref = collection.document(document_id(cursor, prefix="operation"))
            cursor_snapshot = await cursor_ref.get()
            if not cursor_snapshot.exists:
                raise ValueError("operation cursor no longer exists")
            query = query.start_after(cursor_snapshot)
        try:
            snapshots = [item async for item in query.stream()]
        except Exception as exc:
            raise convert_firestore_error("list_sandbox_operations_for_stop", exc) from None
        page = snapshots[:limit]
        next_cursor = (
            (page[-1].to_dict() or {}).get("operation_id")
            if len(snapshots) > limit and page
            else None
        )
        return tuple(item.to_dict() or {} for item in page), next_cursor


def service_operation_store(
    client: firestore.AsyncClient, *, collection_prefix: str = "silk-agent"
) -> ServiceOperationStore:
    return FirestoreServiceOperationStore(client, collection_prefix=collection_prefix)


def _validate_utc(value: datetime, field_name: str) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
