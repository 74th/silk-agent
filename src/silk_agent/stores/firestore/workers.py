"""Firestore worker liveness and capacity directory."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import cast

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional

from silk_agent.core.distributed_models import WorkerRecord
from silk_agent.core.distributed_ports import WorkerDirectory
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import document_id
from silk_agent.stores.firestore.retention import RetentionPolicy, worker_expiry


class FirestoreWorkerDirectory:
    def __init__(
        self,
        client: firestore.AsyncClient,
        *,
        collection_prefix: str = "silk-agent",
        heartbeat_timeout: timedelta = timedelta(seconds=60),
        retention: RetentionPolicy | None = None,
    ):
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("collection_prefix must be non-empty and cannot contain '/'")
        if heartbeat_timeout <= timedelta(0):
            raise ValueError("heartbeat_timeout must be positive")
        self.client = client
        self.collection_prefix = collection_prefix
        self.heartbeat_timeout = heartbeat_timeout
        self.retention = retention or RetentionPolicy()

    def _expire_at(self, heartbeat_at: datetime) -> datetime:
        return worker_expiry(heartbeat_at.astimezone(UTC), retention=self.retention.worker)

    def _worker_ref(self, worker_id: str) -> firestore.AsyncDocumentReference:
        return self.client.collection(f"{self.collection_prefix}_workers").document(
            document_id(worker_id, prefix="worker")
        )

    async def register(self, record: WorkerRecord) -> bool:
        worker_ref = self._worker_ref(record.worker_id)
        transaction = self.client.transaction()

        @async_transactional
        async def write(transaction: firestore.AsyncTransaction) -> bool:
            snapshot = await worker_ref.get(transaction=transaction)
            if snapshot.exists:
                current = WorkerRecord.from_dict(snapshot.to_dict() or {})
                if current.incarnation == record.incarnation:
                    return False
            transaction.set(
                worker_ref,
                {**record.to_dict(), "expire_at": self._expire_at(record.heartbeat_at)},
            )
            return True

        try:
            return await write(transaction)
        except Exception as exc:
            raise convert_firestore_error("register_worker", exc) from None

    async def heartbeat(self, record: WorkerRecord) -> bool:
        worker_ref = self._worker_ref(record.worker_id)
        transaction = self.client.transaction()

        @async_transactional
        async def update(transaction: firestore.AsyncTransaction) -> bool:
            snapshot = await worker_ref.get(transaction=transaction)
            if not snapshot.exists:
                return False
            current = WorkerRecord.from_dict(snapshot.to_dict() or {})
            if (
                current.incarnation != record.incarnation
                or record.heartbeat_at < current.heartbeat_at
            ):
                return False
            transaction.set(
                worker_ref,
                WorkerRecord(
                    worker_id=record.worker_id,
                    incarnation=record.incarnation,
                    capacity=record.capacity,
                    reserved=current.reserved,
                    active_sdk=record.active_sdk,
                    # Drain is admin-owned state; a heartbeat from a Worker with
                    # a stale local record must not accidentally undrain itself.
                    draining=current.draining,
                    heartbeat_at=record.heartbeat_at,
                    drain_deadline_at=current.drain_deadline_at,
                ).to_dict()
                | {"expire_at": self._expire_at(record.heartbeat_at)},
            )
            return True

        try:
            return await update(transaction)
        except Exception as exc:
            raise convert_firestore_error("heartbeat_worker", exc) from None

    async def list_available(self, *, now: datetime) -> Sequence[WorkerRecord]:
        if now.tzinfo is None or now.utcoffset() is None or now.utcoffset() != timedelta(0):
            raise ValueError("now must be timezone-aware UTC")
        cutoff = now.astimezone(UTC) - self.heartbeat_timeout
        workers = self.client.collection(f"{self.collection_prefix}_workers")
        try:
            available: list[WorkerRecord] = []
            async for snapshot in workers.stream():
                record = WorkerRecord.from_dict(cast(dict, snapshot.to_dict() or {}))
                if (
                    not record.draining
                    and record.heartbeat_at >= cutoff
                    and record.heartbeat_at <= now
                    and record.reserved < record.capacity
                ):
                    available.append(record)
            return tuple(available)
        except Exception as exc:
            raise convert_firestore_error("list_available_workers", exc) from None

    async def get_worker(self, worker_id: str) -> WorkerRecord | None:
        try:
            snapshot = await self._worker_ref(worker_id).get()
            if not snapshot.exists:
                return None
            return WorkerRecord.from_dict(cast(dict, snapshot.to_dict() or {}))
        except Exception as exc:
            raise convert_firestore_error("get_worker", exc) from None

    async def list_workers(self) -> Sequence[WorkerRecord]:
        try:
            workers = self.client.collection(f"{self.collection_prefix}_workers")
            records = [
                WorkerRecord.from_dict(cast(dict, snapshot.to_dict() or {}))
                async for snapshot in workers.stream()
            ]
            return tuple(records)
        except Exception as exc:
            raise convert_firestore_error("list_workers", exc) from None

    async def set_cluster_draining(self, draining: bool) -> None:
        if not isinstance(draining, bool):
            raise ValueError("draining must be a bool")
        try:
            await (
                self.client.collection(f"{self.collection_prefix}_system_state")
                .document("distributed")
                .set({"draining": draining, "schema_version": 1})
            )
        except Exception as exc:
            raise convert_firestore_error("set_cluster_draining", exc) from None

    async def is_cluster_draining(self) -> bool:
        try:
            state_collection = self.client.collection(f"{self.collection_prefix}_system_state")
            snapshot = await state_collection.document("distributed").get()
            return bool(snapshot.exists and (snapshot.to_dict() or {}).get("draining") is True)
        except Exception as exc:
            raise convert_firestore_error("get_cluster_draining", exc) from None

    async def set_draining(
        self,
        *,
        worker_id: str,
        incarnation: str,
        draining: bool,
        now: datetime,
        drain_deadline_at: datetime | None = None,
    ) -> WorkerRecord | None:
        if now.tzinfo is None or now.utcoffset() is None or now.utcoffset() != timedelta(0):
            raise ValueError("now must be timezone-aware UTC")
        if not isinstance(draining, bool):
            raise ValueError("draining must be a bool")
        if drain_deadline_at is not None and (
            drain_deadline_at.tzinfo is None
            or drain_deadline_at.utcoffset() is None
            or drain_deadline_at.utcoffset() != timedelta(0)
        ):
            raise ValueError("drain_deadline_at must be timezone-aware UTC")
        worker_ref = self._worker_ref(worker_id)
        transaction = self.client.transaction()

        @async_transactional
        async def update(transaction: firestore.AsyncTransaction) -> WorkerRecord | None:
            snapshot = await worker_ref.get(transaction=transaction)
            if not snapshot.exists:
                return None
            current = WorkerRecord.from_dict(cast(dict, snapshot.to_dict() or {}))
            if current.incarnation != incarnation:
                return None
            updated = WorkerRecord(
                worker_id=current.worker_id,
                incarnation=current.incarnation,
                capacity=current.capacity,
                reserved=current.reserved,
                active_sdk=current.active_sdk,
                draining=draining,
                heartbeat_at=current.heartbeat_at,
                drain_deadline_at=drain_deadline_at if draining else None,
            )
            # An admin action does not move the heartbeat, so it keeps the current expiry.
            expire_at = (snapshot.to_dict() or {}).get("expire_at")
            transaction.set(
                worker_ref,
                {**updated.to_dict(), **({"expire_at": expire_at} if expire_at else {})},
            )
            return updated

        try:
            return await update(transaction)
        except Exception as exc:
            raise convert_firestore_error("set_worker_draining", exc) from None

    async def reserve(self, *, worker_id: str, incarnation: str, reservation_id: str) -> bool:
        worker_ref = self._worker_ref(worker_id)
        incarnation_ref = worker_ref.collection("incarnations").document(
            document_id(incarnation, prefix="incarnation")
        )
        reservation_ref = incarnation_ref.collection("reservations").document(
            document_id(reservation_id, prefix="reservation")
        )
        transaction = self.client.transaction()

        @async_transactional
        async def reserve_slot(transaction: firestore.AsyncTransaction) -> bool:
            worker_snapshot = await worker_ref.get(transaction=transaction)
            reservation_snapshot = await reservation_ref.get(transaction=transaction)
            if not worker_snapshot.exists:
                return False
            record = WorkerRecord.from_dict(worker_snapshot.to_dict() or {})
            if record.incarnation != incarnation or record.draining:
                return False
            if reservation_snapshot.exists:
                return True
            if record.reserved >= record.capacity:
                return False
            transaction.create(
                reservation_ref,
                {"reservation_id": reservation_id, "schema_version": 1},
            )
            transaction.update(worker_ref, {"reserved": record.reserved + 1})
            return True

        try:
            return await reserve_slot(transaction)
        except Exception as exc:
            raise convert_firestore_error("reserve_worker", exc) from None

    async def release_reservation(
        self, *, worker_id: str, incarnation: str, reservation_id: str
    ) -> bool:
        worker_ref = self._worker_ref(worker_id)
        incarnation_ref = worker_ref.collection("incarnations").document(
            document_id(incarnation, prefix="incarnation")
        )
        reservation_ref = incarnation_ref.collection("reservations").document(
            document_id(reservation_id, prefix="reservation")
        )
        transaction = self.client.transaction()

        @async_transactional
        async def release_slot(transaction: firestore.AsyncTransaction) -> bool:
            worker_snapshot = await worker_ref.get(transaction=transaction)
            reservation_snapshot = await reservation_ref.get(transaction=transaction)
            if not worker_snapshot.exists or not reservation_snapshot.exists:
                return False
            record = WorkerRecord.from_dict(worker_snapshot.to_dict() or {})
            if record.incarnation != incarnation:
                return False
            transaction.delete(reservation_ref)
            transaction.update(worker_ref, {"reserved": max(0, record.reserved - 1)})
            return True

        try:
            return await release_slot(transaction)
        except Exception as exc:
            raise convert_firestore_error("release_worker_reservation", exc) from None


def worker_directory(
    client: firestore.AsyncClient,
    *,
    collection_prefix: str = "silk-agent",
    heartbeat_timeout: timedelta = timedelta(seconds=60),
    retention: RetentionPolicy | None = None,
) -> WorkerDirectory:
    return FirestoreWorkerDirectory(
        client,
        collection_prefix=collection_prefix,
        heartbeat_timeout=heartbeat_timeout,
        retention=retention,
    )
