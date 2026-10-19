"""Atomic Control allocation of waiting conversations to live Workers."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import cast

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional

from silk_agent.core.distributed_models import OwnershipToken, WorkerRecord
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import conversation_document_id, document_id
from silk_agent.stores.firestore.workers import HEARTBEAT_FUTURE_TOLERANCE


class FirestoreAllocator:
    """Acquire a Control lease and atomically reserve/assign one conversation."""

    def __init__(
        self,
        client: firestore.AsyncClient,
        *,
        collection_prefix: str = "silk-agent",
        worker_heartbeat_timeout: timedelta = timedelta(seconds=60),
    ):
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("collection_prefix must be non-empty and cannot contain '/'")
        if worker_heartbeat_timeout <= timedelta(0):
            raise ValueError("worker_heartbeat_timeout must be positive")
        self.client = client
        self.collection_prefix = collection_prefix
        self.worker_heartbeat_timeout = worker_heartbeat_timeout

    def _collection(self, name: str) -> firestore.AsyncCollectionReference:
        return self.client.collection(f"{self.collection_prefix}_{name}")

    async def allocate(
        self,
        *,
        allocator_id: str,
        conversation_key: str,
        candidates: Sequence[WorkerRecord],
        now: datetime,
        allocator_lease_expires_at: datetime,
        worker_lease_expires_at: datetime,
    ) -> OwnershipToken | None:
        _validate_utc(now, "now")
        _validate_utc(allocator_lease_expires_at, "allocator_lease_expires_at")
        _validate_utc(worker_lease_expires_at, "worker_lease_expires_at")
        if not allocator_id or allocator_lease_expires_at <= now:
            raise ValueError("allocator_id and a future allocator lease are required")
        if worker_lease_expires_at <= now:
            raise ValueError("worker lease must expire in the future")

        conversation_id = conversation_document_id(conversation_key)
        conversation_ref = self._collection("conversations").document(conversation_id)
        owner_ref = self._collection("ownership").document(conversation_id)
        role_lease_ref = self._collection("control_leases").document("allocator")
        system_state_ref = self._collection("system_state").document("distributed")
        worker_refs = [
            self._collection("workers").document(document_id(item.worker_id, prefix="worker"))
            for item in candidates
        ]
        reservation_refs = [
            worker_ref.collection("incarnations")
            .document(document_id(candidate.incarnation, prefix="incarnation"))
            .collection("reservations")
            .document(document_id(conversation_key, prefix="reservation"))
            for worker_ref, candidate in zip(worker_refs, candidates, strict=True)
        ]
        # Competing allocators contend on the same worker and conversation documents.
        transaction = self.client.transaction(max_attempts=15)

        @async_transactional
        async def assign(transaction: firestore.AsyncTransaction) -> OwnershipToken | None:
            lease_snapshot = await role_lease_ref.get(transaction=transaction)
            system_state_snapshot = await system_state_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            owner_snapshot = await owner_ref.get(transaction=transaction)
            worker_snapshots = []
            reservation_snapshots = []
            for worker_ref, reservation_ref in zip(worker_refs, reservation_refs, strict=True):
                worker_snapshots.append(await worker_ref.get(transaction=transaction))
                reservation_snapshots.append(await reservation_ref.get(transaction=transaction))

            if lease_snapshot.exists:
                lease_data = lease_snapshot.to_dict() or {}
                saved_expiry = lease_data.get("expires_at")
                if isinstance(saved_expiry, datetime):
                    lease_expiry = saved_expiry
                elif isinstance(saved_expiry, str):
                    lease_expiry = datetime.fromisoformat(saved_expiry)
                else:
                    return None
                current_allocator = lease_data.get("allocator_id")
                if lease_expiry > now and current_allocator != allocator_id:
                    return None
            if (
                system_state_snapshot.exists
                and (system_state_snapshot.to_dict() or {}).get("draining") is True
            ):
                return None
            if not conversation_snapshot.exists:
                return None
            conversation = conversation_snapshot.to_dict() or {}
            if conversation.get("allocation_state") != "waiting":
                return None
            generation = conversation.get("conversation_generation")
            if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
                return None

            previous_epoch = 0
            if owner_snapshot.exists:
                old_owner = owner_snapshot.to_dict() or {}
                if old_owner.get("phase") != "released":
                    # Expiry does not prove that the previous SDK process stopped.
                    return None
                old_epoch = old_owner.get("epoch", 0)
                if not isinstance(old_epoch, int) or isinstance(old_epoch, bool) or old_epoch < 0:
                    return None
                previous_epoch = old_epoch

            eligible: list[tuple[WorkerRecord, firestore.AsyncDocumentReference]] = []
            for candidate, snapshot, reservation_snapshot, worker_ref in zip(
                candidates,
                worker_snapshots,
                reservation_snapshots,
                worker_refs,
                strict=True,
            ):
                if not snapshot.exists:
                    continue
                current = WorkerRecord.from_dict(cast(dict, snapshot.to_dict() or {}))
                if (
                    current.incarnation != candidate.incarnation
                    or current.draining
                    or current.heartbeat_at > now + HEARTBEAT_FUTURE_TOLERANCE
                    or current.heartbeat_at < now - self.worker_heartbeat_timeout
                    or current.reserved >= current.capacity
                ):
                    continue
                if reservation_snapshot.exists:
                    continue
                eligible.append((current, worker_ref))
            if not eligible:
                return None
            # The Worker with the fewest reservations (the first of equals): filling the first
            # in the list would pile conversations on one Worker, and on a broken one first.
            selected = min(eligible, key=lambda item: item[0].reserved)

            worker, worker_ref = selected
            reservation_ref = (
                worker_ref.collection("incarnations")
                .document(document_id(worker.incarnation, prefix="incarnation"))
                .collection("reservations")
                .document(document_id(conversation_key, prefix="reservation"))
            )
            token = OwnershipToken(
                conversation_key=conversation_key,
                worker_id=worker.worker_id,
                worker_incarnation=worker.incarnation,
                epoch=previous_epoch + 1,
                conversation_generation=generation,
                expires_at=worker_lease_expires_at,
            )
            transaction.set(
                role_lease_ref,
                {"allocator_id": allocator_id, "expires_at": allocator_lease_expires_at},
            )
            transaction.set(reservation_ref, {"conversation_key": conversation_key})
            transaction.update(worker_ref, {"reserved": worker.reserved + 1})
            transaction.set(owner_ref, {**token.to_dict(), "phase": "assigned"})
            transaction.update(conversation_ref, {"allocation_state": "assigned"})
            return token

        try:
            return await assign(transaction)
        except Exception as exc:
            raise convert_firestore_error("allocate_conversation", exc) from None


def _validate_utc(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    if value.astimezone(UTC) != value:
        raise ValueError(f"{field_name} must be UTC")
