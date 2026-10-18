"""Transactional conversation ownership leases with worker capacity accounting."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional
from google.cloud.firestore_v1.base_query import FieldFilter

from silk_agent.core.distributed_models import OwnershipToken, WorkerRecord
from silk_agent.core.distributed_ports import OwnershipStore
from silk_agent.stores.firestore.codec import decode_record
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import conversation_document_id, document_id


class FirestoreOwnershipStore:
    """Owns assigned/active/released transitions; expiry alone never transfers ownership."""

    def __init__(self, client: firestore.AsyncClient, *, collection_prefix: str = "silk-agent"):
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("collection_prefix must be non-empty and cannot contain '/'")
        self.client = client
        self.collection_prefix = collection_prefix

    def _collection(self, name: str) -> firestore.AsyncCollectionReference:
        return self.client.collection(f"{self.collection_prefix}_{name}")

    def _refs(self, conversation_key: str):
        conversation_id = conversation_document_id(conversation_key)
        return (
            self._collection("ownership").document(conversation_id),
            self._collection("conversations").document(conversation_id),
        )

    def _worker_refs(self, worker_id: str, incarnation: str, conversation_key: str):
        worker_ref = self._collection("workers").document(document_id(worker_id, prefix="worker"))
        incarnation_ref = worker_ref.collection("incarnations").document(
            document_id(incarnation, prefix="incarnation")
        )
        reservation_ref = incarnation_ref.collection("reservations").document(
            document_id(conversation_key, prefix="reservation")
        )
        return worker_ref, reservation_ref

    async def get(self, conversation_key: str) -> OwnershipToken | None:
        owner_ref, _ = self._refs(conversation_key)
        try:
            snapshot = await owner_ref.get()
            if not snapshot.exists:
                return None
            data = snapshot.to_dict() or {}
            if data.get("phase") not in {"assigned", "active", "recovering"}:
                return None
            return decode_record(data, OwnershipToken)
        except Exception as exc:
            raise convert_firestore_error("get_ownership", exc) from None

    async def list_expired(
        self, *, before: datetime, limit: int = 100
    ) -> tuple[OwnershipToken, ...]:
        """Ownerships whose lease ended before ``before`` and that are not yet released.

        Includes ``recovering`` ones so blocked recoveries are retried by Control.
        """
        _validate_utc(before, "before")
        if limit < 1:
            raise ValueError("limit must be positive")
        # Live phases only (bounded by Worker capacity); released documents are never scanned.
        query = self._collection("ownership").where(
            filter=FieldFilter("phase", "in", ["assigned", "active", "recovering"])
        )
        try:
            tokens = []
            async for snapshot in query.stream():
                token = decode_record(snapshot.to_dict() or {}, OwnershipToken)
                if token.expires_at < before:
                    tokens.append(token)
                    if len(tokens) >= limit:
                        break
            return tuple(tokens)
        except Exception as exc:
            raise convert_firestore_error("list_expired_ownership", exc) from None

    async def list_for_worker(
        self, *, worker_id: str, worker_incarnation: str
    ) -> tuple[OwnershipToken, ...]:
        if not worker_id or not worker_incarnation:
            raise ValueError("worker identity must be non-empty")
        worker_ref = self._collection("workers").document(document_id(worker_id, prefix="worker"))
        try:
            tokens = []
            async for reservation in (
                worker_ref.collection("incarnations")
                .document(document_id(worker_incarnation, prefix="incarnation"))
                .collection("reservations")
                .stream()
            ):
                data = reservation.to_dict() or {}
                conversation_key = data.get("conversation_key")
                if not isinstance(conversation_key, str) or not conversation_key:
                    continue
                token = await self.get(conversation_key)
                if (
                    token is not None
                    and token.worker_id == worker_id
                    and token.worker_incarnation == worker_incarnation
                ):
                    tokens.append(token)
            return tuple(tokens)
        except Exception as exc:
            raise convert_firestore_error("list_worker_ownership", exc) from None

    async def acquire(
        self,
        *,
        conversation_key: str,
        worker_id: str,
        worker_incarnation: str,
        conversation_generation: int,
        now: datetime,
        lease_expires_at: datetime,
    ) -> OwnershipToken | None:
        _validate_utc(now, "now")
        _validate_utc(lease_expires_at, "lease_expires_at")
        if lease_expires_at <= now:
            raise ValueError("lease_expires_at must be in the future")
        owner_ref, conversation_ref = self._refs(conversation_key)
        worker_ref, reservation_ref = self._worker_refs(
            worker_id, worker_incarnation, conversation_key
        )
        transaction = self.client.transaction()

        @async_transactional
        async def assign(transaction: firestore.AsyncTransaction) -> OwnershipToken | None:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            worker_snapshot = await worker_ref.get(transaction=transaction)
            reservation_snapshot = await reservation_ref.get(transaction=transaction)
            if not conversation_snapshot.exists or not worker_snapshot.exists:
                return None
            conversation = conversation_snapshot.to_dict() or {}
            if (
                conversation.get("conversation_generation") != conversation_generation
                or conversation.get("allocation_state") != "waiting"
            ):
                return None
            epoch = 0
            if owner_snapshot.exists:
                owner_data = owner_snapshot.to_dict() or {}
                if owner_data.get("phase") != "released":
                    return None
                old_epoch = owner_data.get("epoch", 0)
                if not isinstance(old_epoch, int) or isinstance(old_epoch, bool) or old_epoch < 0:
                    return None
                epoch = old_epoch
            worker = WorkerRecord.from_dict(cast(dict, worker_snapshot.to_dict() or {}))
            if worker.incarnation != worker_incarnation or worker.draining:
                return None
            if reservation_snapshot.exists:
                return None
            if worker.reserved >= worker.capacity:
                return None
            token = OwnershipToken(
                conversation_key,
                worker_id,
                worker_incarnation,
                epoch + 1,
                conversation_generation,
                lease_expires_at,
            )
            transaction.create(
                reservation_ref, {"conversation_key": conversation_key, "schema_version": 1}
            )
            transaction.update(worker_ref, {"reserved": worker.reserved + 1})
            transaction.set(owner_ref, {**token.to_dict(), "phase": "assigned"})
            transaction.update(conversation_ref, {"allocation_state": "assigned"})
            return token

        try:
            return await assign(transaction)
        except Exception as exc:
            raise convert_firestore_error("acquire_ownership", exc) from None

    async def activate(self, token: OwnershipToken, *, now: datetime) -> bool:
        _validate_utc(now, "now")
        owner_ref, conversation_ref = self._refs(token.conversation_key)
        worker_ref, _ = self._worker_refs(
            token.worker_id, token.worker_incarnation, token.conversation_key
        )
        transaction = self.client.transaction()

        @async_transactional
        async def activate_owner(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            worker_snapshot = await worker_ref.get(transaction=transaction)
            if (
                not owner_snapshot.exists
                or not conversation_snapshot.exists
                or not worker_snapshot.exists
            ):
                return False
            owner_data = owner_snapshot.to_dict() or {}
            saved = decode_record(owner_data, OwnershipToken)
            conversation = conversation_snapshot.to_dict() or {}
            worker = WorkerRecord.from_dict(cast(dict, worker_snapshot.to_dict() or {}))
            if (
                saved != token
                or owner_data.get("phase") != "assigned"
                or token.expires_at <= now
                or conversation.get("conversation_generation") != token.conversation_generation
                or conversation.get("allocation_state") != "assigned"
                or worker.incarnation != token.worker_incarnation
                or worker.active_sdk >= worker.capacity
            ):
                return False
            transaction.update(owner_ref, {"phase": "active", "sdk_active": True})
            transaction.update(worker_ref, {"active_sdk": worker.active_sdk + 1})
            return True

        try:
            return await activate_owner(transaction)
        except Exception as exc:
            raise convert_firestore_error("activate_ownership", exc) from None

    async def activate_control(self, token: OwnershipToken, *, now: datetime) -> bool:
        _validate_utc(now, "now")
        owner_ref, conversation_ref = self._refs(token.conversation_key)
        worker_ref, _ = self._worker_refs(
            token.worker_id, token.worker_incarnation, token.conversation_key
        )
        transaction = self.client.transaction()

        @async_transactional
        async def activate_control_owner(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            worker_snapshot = await worker_ref.get(transaction=transaction)
            if (
                not owner_snapshot.exists
                or not conversation_snapshot.exists
                or not worker_snapshot.exists
            ):
                return False
            owner_data = owner_snapshot.to_dict() or {}
            saved = decode_record(owner_data, OwnershipToken)
            conversation = conversation_snapshot.to_dict() or {}
            worker = WorkerRecord.from_dict(cast(dict, worker_snapshot.to_dict() or {}))
            if (
                saved != token
                or owner_data.get("phase") != "assigned"
                or token.expires_at <= now
                or conversation.get("conversation_generation") != token.conversation_generation
                or conversation.get("control_only") is not True
                or worker.incarnation != token.worker_incarnation
            ):
                return False
            transaction.update(owner_ref, {"phase": "active", "sdk_active": False})
            return True

        try:
            return await activate_control_owner(transaction)
        except Exception as exc:
            raise convert_firestore_error("activate_control_ownership", exc) from None

    async def renew(
        self, token: OwnershipToken, *, now: datetime, lease_expires_at: datetime
    ) -> OwnershipToken | None:
        _validate_utc(now, "now")
        _validate_utc(lease_expires_at, "lease_expires_at")
        if token.expires_at <= now or lease_expires_at <= now:
            return None
        if lease_expires_at <= token.expires_at:
            raise ValueError("renewed lease must extend the current expiry")
        owner_ref, conversation_ref = self._refs(token.conversation_key)
        transaction = self.client.transaction()

        @async_transactional
        async def renew_owner(transaction: firestore.AsyncTransaction) -> OwnershipToken | None:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            if not owner_snapshot.exists or not conversation_snapshot.exists:
                return None
            owner_data = owner_snapshot.to_dict() or {}
            saved = decode_record(owner_data, OwnershipToken)
            conversation = conversation_snapshot.to_dict() or {}
            if (
                saved != token
                or owner_data.get("phase") != "active"
                or token.expires_at <= now
                or conversation.get("conversation_generation") != token.conversation_generation
            ):
                return None
            renewed = OwnershipToken(
                token.conversation_key,
                token.worker_id,
                token.worker_incarnation,
                token.epoch,
                token.conversation_generation,
                lease_expires_at,
            )
            transaction.update(owner_ref, {"expires_at": lease_expires_at.isoformat()})
            return renewed

        try:
            return await renew_owner(transaction)
        except Exception as exc:
            raise convert_firestore_error("renew_ownership", exc) from None

    async def release_unactivated(self, token: OwnershipToken, *, now: datetime) -> bool:
        """Release an expired assignment that its Worker never activated.

        A Worker moves ``assigned`` to ``active`` before it starts an SDK, so an expired
        ``assigned`` ownership has no process to prove stopped.
        """
        return await self.release(token, now=now, _expired_unactivated=True)

    async def release(
        self, token: OwnershipToken, *, now: datetime, _expired_unactivated: bool = False
    ) -> bool:
        _validate_utc(now, "now")
        owner_ref, conversation_ref = self._refs(token.conversation_key)
        worker_ref, reservation_ref = self._worker_refs(
            token.worker_id, token.worker_incarnation, token.conversation_key
        )
        transaction = self.client.transaction()

        @async_transactional
        async def release_owner(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            worker_snapshot = await worker_ref.get(transaction=transaction)
            reservation_snapshot = await reservation_ref.get(transaction=transaction)
            if not owner_snapshot.exists or not conversation_snapshot.exists:
                return False
            owner_data = owner_snapshot.to_dict() or {}
            saved = decode_record(owner_data, OwnershipToken)
            phase = owner_data.get("phase")
            worker = _worker_if_exists(worker_snapshot)
            lease_ok = token.expires_at <= now if _expired_unactivated else token.expires_at > now
            allowed_phases = {"assigned"} if _expired_unactivated else {"assigned", "active"}
            if saved != token or phase not in allowed_phases or not lease_ok:
                return False
            # A live Worker must still be the incarnation that holds the token. An unactivated
            # assignment started no process, so a replaced incarnation does not block it.
            if (
                worker is not None
                and not _expired_unactivated
                and worker.incarnation != token.worker_incarnation
            ):
                return False
            conversation = conversation_snapshot.to_dict() or {}
            if conversation.get("conversation_generation") != token.conversation_generation:
                return False
            unfinished_query = (
                conversation_ref.collection("inputs")
                .where(
                    filter=FieldFilter(
                        "delivery_state", "in", ["pending", "dispatching", "queued", "accepted"]
                    )
                )
                .limit(1)
            )
            unfinished = [item async for item in unfinished_query.stream(transaction=transaction)]
            transaction.update(
                owner_ref,
                {"phase": "released", "released_at": now.isoformat()},
            )
            if reservation_snapshot.exists:
                transaction.delete(reservation_ref)
            _release_worker_counters(
                transaction,
                worker_ref,
                worker,
                token,
                active=phase == "active" and owner_data.get("sdk_active", True),
            )
            transaction.update(
                conversation_ref,
                {"allocation_state": "waiting" if unfinished else "idle"},
            )
            return True

        try:
            return await release_owner(transaction)
        except Exception as exc:
            raise convert_firestore_error("release_ownership", exc) from None

    async def begin_recovery(self, token: OwnershipToken, *, now: datetime) -> bool:
        _validate_utc(now, "now")
        if token.expires_at > now:
            return False
        owner_ref, conversation_ref = self._refs(token.conversation_key)
        transaction = self.client.transaction()

        @async_transactional
        async def recover(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            if not owner_snapshot.exists or not conversation_snapshot.exists:
                return False
            owner_data = owner_snapshot.to_dict() or {}
            conversation = conversation_snapshot.to_dict() or {}
            if (
                decode_record(owner_data, OwnershipToken) != token
                or owner_data.get("phase") not in {"assigned", "active", "recovering"}
                or conversation.get("conversation_generation") != token.conversation_generation
            ):
                return False
            transaction.update(
                owner_ref,
                {
                    "phase": "recovering",
                    "recovery_prior_phase": owner_data.get("phase"),
                    "recovery_requested_at": now.isoformat(),
                    "recovery_state": "quiescing",
                },
            )
            transaction.update(
                conversation_ref,
                {"allocation_state": "recovering", "recovery_state": "quiescing"},
            )
            return True

        try:
            return await recover(transaction)
        except Exception as exc:
            raise convert_firestore_error("begin_ownership_recovery", exc) from None

    async def mark_recovery_blocked(
        self, token: OwnershipToken, *, reason: str, now: datetime
    ) -> bool:
        _validate_utc(now, "now")
        if not reason or len(reason) > 80:
            raise ValueError("recovery reason must be short and non-empty")
        owner_ref, conversation_ref = self._refs(token.conversation_key)
        transaction = self.client.transaction()

        @async_transactional
        async def block(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            if not owner_snapshot.exists or not conversation_snapshot.exists:
                return False
            owner_data = owner_snapshot.to_dict() or {}
            if (
                decode_record(owner_data, OwnershipToken) != token
                or owner_data.get("phase") != "recovering"
            ):
                return False
            transaction.update(
                owner_ref,
                {"recovery_state": "blocked", "recovery_block_reason": reason},
            )
            transaction.update(
                conversation_ref,
                {
                    "allocation_state": "recovery_blocked",
                    "recovery_state": "blocked",
                    "recovery_block_reason": reason,
                },
            )
            return True

        try:
            return await block(transaction)
        except Exception as exc:
            raise convert_firestore_error("block_ownership_recovery", exc) from None

    async def confirm_stopped(
        self,
        token: OwnershipToken,
        *,
        evidence_id: str,
        worker_incarnation: str,
        now: datetime,
    ) -> bool:
        _validate_utc(now, "now")
        if not evidence_id or worker_incarnation != token.worker_incarnation:
            raise ValueError("stop evidence must identify the assigned Worker incarnation")
        owner_ref, conversation_ref = self._refs(token.conversation_key)
        worker_ref, reservation_ref = self._worker_refs(
            token.worker_id, token.worker_incarnation, token.conversation_key
        )
        transaction = self.client.transaction()

        @async_transactional
        async def release_after_stop(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            worker_snapshot = await worker_ref.get(transaction=transaction)
            reservation_snapshot = await reservation_ref.get(transaction=transaction)
            if not owner_snapshot.exists or not conversation_snapshot.exists:
                return False
            owner_data = owner_snapshot.to_dict() or {}
            conversation = conversation_snapshot.to_dict() or {}
            if (
                decode_record(owner_data, OwnershipToken) != token
                or owner_data.get("phase") != "recovering"
                or conversation.get("conversation_generation") != token.conversation_generation
            ):
                return False
            uncertain_query = (
                conversation_ref.collection("inputs")
                .where(filter=FieldFilter("delivery_state", "in", ["dispatching", "accepted"]))
                .limit(400)
            )
            uncertain_result = await transaction.get(uncertain_query)
            uncertain_inputs = [item async for item in uncertain_result]
            # dispatching/accepted inputs are turned into uncertain below, so only work that is
            # still waiting for a Worker (pending/queued) decides "waiting" versus "idle".
            unfinished_query = (
                conversation_ref.collection("inputs")
                .where(filter=FieldFilter("delivery_state", "in", ["pending", "queued"]))
                .limit(1)
            )
            unfinished_result = await transaction.get(unfinished_query)
            unfinished = [item async for item in unfinished_result]
            commands_query = (
                self._collection("commands")
                .where(filter=FieldFilter("conversation_key", "==", token.conversation_key))
                .where(filter=FieldFilter("origin_epoch", "==", token.epoch))
                .limit(100)
            )
            commands_result = await transaction.get(commands_query)
            commands = [item async for item in commands_result]
            foreground_commands = [
                item
                for item in commands
                if (item_data := item.to_dict() or {}).get("delivery_mode", "foreground")
                == "foreground"
                and item_data.get("state") in {"intent", "running", "cancel_requested"}
            ]
            worker = _worker_if_exists(worker_snapshot)
            transaction.update(
                owner_ref,
                {
                    "phase": "released",
                    "released_at": now.isoformat(),
                    "recovery_state": "confirmed_stopped",
                    "quiesce_evidence": {
                        "evidence_id": evidence_id,
                        "worker_incarnation": worker_incarnation,
                        "confirmed_at": now.isoformat(),
                    },
                },
            )
            for input_snapshot in uncertain_inputs:
                transaction.update(
                    input_snapshot.reference,
                    {"delivery_state": "uncertain", "recovery_evidence_id": evidence_id},
                )
            for command_snapshot in foreground_commands:
                transaction.update(
                    command_snapshot.reference,
                    {
                        "delivery_mode": "orphaned",
                        "orphaned_at": now,
                        "recovery_evidence_id": evidence_id,
                        "updated_at": now,
                    },
                )
            if reservation_snapshot.exists:
                transaction.delete(reservation_ref)
            _release_worker_counters(
                transaction,
                worker_ref,
                worker,
                token,
                active=owner_data.get("recovery_prior_phase") == "active"
                and owner_data.get("sdk_active", True),
            )
            transaction.update(
                conversation_ref,
                {
                    "allocation_state": "waiting" if unfinished else "idle",
                    "recovery_state": "confirmed_stopped",
                },
            )
            return True

        try:
            return await release_after_stop(transaction)
        except Exception as exc:
            raise convert_firestore_error("confirm_worker_stopped", exc) from None


def _worker_if_exists(snapshot: Any) -> WorkerRecord | None:
    """The Worker record, or None once it was removed (its retention period passed)."""
    if not snapshot.exists:
        return None
    return WorkerRecord.from_dict(cast(dict, snapshot.to_dict() or {}))


def _release_worker_counters(
    transaction: firestore.AsyncTransaction,
    worker_ref: firestore.AsyncDocumentReference,
    worker: WorkerRecord | None,
    token: OwnershipToken,
    *,
    active: bool,
) -> None:
    """Give back the reservation (and SDK slot) of `token` on its Worker.

    The counters belong to the incarnation recorded on the Worker document. A newer
    incarnation (the container restarted) or a removed record has nothing of this token's to
    give back, and decrementing would free capacity somebody else holds.
    """
    if worker is None or worker.incarnation != token.worker_incarnation:
        return
    transaction.update(
        worker_ref,
        {
            "reserved": max(0, worker.reserved - 1),
            "active_sdk": max(0, worker.active_sdk - (1 if active else 0)),
        },
    )


def ownership_matches(
    owner_data: dict[str, object],
    conversation_data: dict[str, object],
    token: OwnershipToken,
    *,
    now: datetime,
    required_phase: str = "active",
) -> bool:
    """Apply the shared owner epoch/incarnation/generation/lease gate to transaction reads."""
    try:
        saved = decode_record(owner_data, OwnershipToken)
    except (TypeError, ValueError):
        return False
    return (
        saved == token
        and owner_data.get("phase") == required_phase
        and token.expires_at > now
        and conversation_data.get("conversation_generation") == token.conversation_generation
    )


def _validate_utc(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    if value.astimezone(UTC) != value:
        raise ValueError(f"{field_name} must be UTC")


def ownership_store(
    client: firestore.AsyncClient, *, collection_prefix: str = "silk-agent"
) -> OwnershipStore:
    return FirestoreOwnershipStore(client, collection_prefix=collection_prefix)
