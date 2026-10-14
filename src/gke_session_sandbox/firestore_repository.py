"""Transactional Sandbox records and resource leases in Firestore."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional
from google.cloud.firestore_v1.base_query import FieldFilter

from .errors import (
    RepositoryCapacityError,
    RepositoryConflictError,
    SessionLeaseHeldError,
    SessionLeaseLostError,
    StaleGenerationError,
)
from .file_operations import FileOperation, change_file_operation
from .repository import LeaseToken, StoredSession
from .repository_port import SessionRepository
from .types import (
    CommandRecord,
    CommandState,
    SessionEvent,
    SessionKey,
    SessionRecord,
    SessionState,
    utc_now,
)


def encode_session(record: SessionRecord, revision: int) -> dict[str, Any]:
    """Preserve the public record and use native UTC timestamps for queries."""
    validated = SessionRecord.model_validate(record.model_dump())
    data = _native_dates(validated.model_dump(mode="json"))
    return {**data, "schema_version": 1, "resource_version": revision}


def decode_session(data: dict[str, Any], key_hash: str) -> StoredSession:
    values = dict(data)
    schema = values.pop("schema_version", None)
    revision = values.pop("resource_version", None)
    if schema != 1 or isinstance(schema, bool):
        raise RepositoryConflictError("unsupported Sandbox record schema")
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
        raise RepositoryConflictError("invalid Sandbox record revision")
    record = SessionRecord.model_validate(values)
    if record.key_hash != key_hash:
        raise RepositoryConflictError("Sandbox record key does not match its document")
    return StoredSession(record, str(revision))


def _native_dates(value: Any, name: str = "") -> Any:
    if isinstance(value, dict):
        return {key: _native_dates(item, key) for key, item in value.items()}
    if isinstance(value, list):
        return [_native_dates(item) for item in value]
    if isinstance(value, str) and (name.endswith("_at") or name in {"idle_since", "retry_after"}):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    return value


class FirestoreSessionRepository:
    """Validate lease epoch, generation, and revision in each write transaction.

    The caller owns the AsyncClient; closing a manager does not close a client
    shared by its monitor or other store adapters.
    """

    def __init__(
        self,
        client: firestore.AsyncClient,
        *,
        collection_prefix: str = "silk-agent",
        lease_duration_seconds: int = 60,
        max_pending_events: int = 100,
        record_limit_bytes: int = 900 * 1024,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("invalid collection prefix")
        if lease_duration_seconds <= 3 or max_pending_events < 1 or record_limit_bytes < 1:
            raise ValueError("invalid repository limits")
        self.client = client
        self._prefix = collection_prefix
        self._duration = lease_duration_seconds
        self._max_events = max_pending_events
        self._record_limit = record_limit_bytes
        self._clock = clock

    async def close(self) -> None:
        pass

    def _refs(self, key_hash: str):
        if re.fullmatch(r"[0-9a-f]{64}", key_hash) is None:
            raise ValueError("invalid session key hash")
        return (
            self.client.collection(f"{self._prefix}_sandboxes").document(key_hash),
            self.client.collection(f"{self._prefix}_resource_leases").document(
                f"sandbox-{key_hash}"
            ),
        )

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() != timedelta(0):
            raise ValueError("repository clock must return UTC")
        return now

    async def acquire_lease(self, key: SessionKey, holder_identity: str) -> LeaseToken:
        return await self.acquire_lease_by_hash(key.stable_hash(), holder_identity)

    async def acquire_lease_by_hash(self, key_hash: str, holder_identity: str) -> LeaseToken:
        if not holder_identity.strip():
            raise ValueError("holder_identity must not be blank")
        _, lease_ref = self._refs(key_hash)

        @async_transactional
        async def acquire(transaction):
            snapshot = await lease_ref.get(transaction=transaction)
            data = snapshot.to_dict() or {}
            now = self._now()
            epoch = data.get("epoch", 0)
            if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch < 0:
                raise SessionLeaseLostError("invalid Sandbox lease epoch")
            live = snapshot.exists and data.get("expires_at", now) > now
            if live and data.get("holder_identity") != holder_identity:
                raise SessionLeaseHeldError("Sandbox lease is held by another service")
            if not live:
                epoch += 1
            transaction.set(
                lease_ref,
                {
                    "key_hash": key_hash,
                    "holder_identity": holder_identity,
                    "epoch": epoch,
                    "expires_at": now + timedelta(seconds=self._duration),
                    "schema_version": 1,
                },
            )
            return LeaseToken(key_hash, holder_identity, epoch)

        return await acquire(self.client.transaction())

    def _assert_lease(self, snapshot, token: LeaseToken) -> None:
        data = snapshot.to_dict() or {}
        now = self._now()
        if (
            not snapshot.exists
            or data.get("holder_identity") != token.holder_identity
            or data.get("epoch") != token.epoch
            or data.get("expires_at", now) <= now
        ):
            raise SessionLeaseLostError("Sandbox lease is no longer held by this service")

    async def renew_lease(self, token: LeaseToken) -> LeaseToken:
        await self._update_lease(token, release=False)
        return token

    async def release_lease(self, token: LeaseToken) -> None:
        await self._update_lease(token, release=True)

    async def _update_lease(self, token: LeaseToken, *, release: bool) -> None:
        _, lease_ref = self._refs(token.key_hash)

        @async_transactional
        async def update(transaction):
            snapshot = await lease_ref.get(transaction=transaction)
            self._assert_lease(snapshot, token)
            now = self._now()
            transaction.update(
                lease_ref,
                {
                    "holder_identity": "" if release else token.holder_identity,
                    "expires_at": now if release else now + timedelta(seconds=self._duration),
                },
            )

        await update(self.client.transaction())

    async def get(self, key: SessionKey) -> StoredSession | None:
        return await self.get_by_hash(key.stable_hash())

    async def get_by_hash(self, key_hash: str) -> StoredSession | None:
        record_ref, _ = self._refs(key_hash)
        snapshot = await record_ref.get()
        return decode_session(snapshot.to_dict() or {}, key_hash) if snapshot.exists else None

    async def list_records(self) -> tuple[StoredSession, ...]:
        collection = self.client.collection(f"{self._prefix}_sandboxes")
        return tuple(
            [decode_session(item.to_dict() or {}, item.id) async for item in collection.stream()]
        )

    async def list_reconciliation_candidates(
        self, *, limit: int = 100, cursor: str | None = None
    ) -> tuple[tuple[StoredSession, ...], str | None]:
        if limit < 1:
            raise ValueError("page limit must be positive")
        collection = self.client.collection(f"{self._prefix}_sandboxes")
        states = [
            SessionState.READY.value,
            SessionState.SUSPENDED.value,
            SessionState.SUSPENDING.value,
            SessionState.RESUMING.value,
            SessionState.TERMINATING.value,
        ]
        query = collection.where(filter=FieldFilter("state", "in", states))
        query = query.order_by("__name__").limit(limit + 1)
        if cursor is not None:
            query = query.start_after({"__name__": cursor})
        snapshots = [item async for item in query.stream()]
        page = snapshots[:limit]
        next_cursor = page[-1].id if len(snapshots) > limit and page else None
        return (
            tuple(decode_session(item.to_dict() or {}, item.id) for item in page),
            next_cursor,
        )

    async def _mutate(self, token: LeaseToken, transform):
        record_ref, lease_ref = self._refs(token.key_hash)

        @async_transactional
        async def update(transaction):
            lease = await lease_ref.get(transaction=transaction)
            snapshot = await record_ref.get(transaction=transaction)
            self._assert_lease(lease, token)
            current = (
                decode_session(snapshot.to_dict() or {}, token.key_hash)
                if snapshot.exists
                else None
            )
            record, result = transform(current)
            if record is not None:
                if record.key_hash != token.key_hash:
                    raise SessionLeaseLostError("Sandbox lease belongs to a different session")
                if current is not None and record.generation < current.record.generation:
                    raise StaleGenerationError("Sandbox generation cannot move backwards")
                revision = int(current.resource_version) + 1 if current is not None else 1
                values = encode_session(record, revision)
                if (
                    sum(event.acked_at is None for event in record.pending_events)
                    > self._max_events
                ):
                    raise RepositoryCapacityError("pending event count exceeds configured limit")
                if len(json.dumps(values, default=str).encode()) > self._record_limit:
                    raise RepositoryCapacityError("Sandbox record exceeds Firestore size limit")
                transaction.set(record_ref, values)
                if result is None:
                    result = StoredSession(record, str(revision))
            return result

        return await update(self.client.transaction())

    async def create(self, token: LeaseToken, record: SessionRecord) -> StoredSession:
        def transform(current):
            if current is not None:
                raise RepositoryConflictError("Sandbox record already exists")
            return record, None

        return await self._mutate(token, transform)

    async def compare_and_swap(
        self, token: LeaseToken, record: SessionRecord, *, expected_resource_version: str
    ) -> StoredSession:
        def transform(current):
            if current is None or current.resource_version != expected_resource_version:
                raise RepositoryConflictError("Sandbox record version changed")
            return record, None

        return await self._mutate(token, transform)

    async def reserve_command(
        self, token: LeaseToken, *, generation: int, command_id: str, runtime_instance_id: str
    ) -> tuple[CommandRecord, bool]:
        def transform(current):
            record = _require_record(current)
            if record.generation != generation:
                raise StaleGenerationError("command refers to an older Sandbox generation")
            for command in record.active_commands:
                if command.command_id == command_id:
                    if command.generation != generation:
                        raise StaleGenerationError("command ID belongs to another generation")
                    return None, (command, False)
            command = CommandRecord(
                command_id=command_id,
                generation=generation,
                runtime_instance_id=runtime_instance_id,
                state=CommandState.RUNNING,
                accepted_at=self._now(),
            )
            return record.model_copy(
                update={"active_commands": (*record.active_commands, command)}
            ), (command, True)

        return await self._mutate(token, transform)

    async def append_event_once(self, token: LeaseToken, event: SessionEvent) -> bool:
        def transform(current):
            record = _require_record(current)
            if event.generation != record.generation:
                raise StaleGenerationError("event refers to an older Sandbox generation")
            if any(item.event_id == event.event_id for item in record.pending_events):
                return None, False
            return record.model_copy(
                update={"pending_events": (*record.pending_events, event)}
            ), True

        return await self._mutate(token, transform)

    async def acknowledge_event(self, token: LeaseToken, event_id: str) -> bool:
        def transform(current):
            record = _require_record(current)
            changed = False
            events = []
            for event in record.pending_events:
                if event.event_id == event_id and event.acked_at is None:
                    if event.generation != record.generation:
                        raise StaleGenerationError("event belongs to an older Sandbox generation")
                    event = event.model_copy(update={"acked_at": self._now()})
                    changed = True
                events.append(event)
            return (
                (record.model_copy(update={"pending_events": tuple(events)}), True)
                if changed
                else (None, False)
            )

        return await self._mutate(token, transform)

    async def record_operation(
        self, token: LeaseToken, *, operation_id: str | None, operation_phase: str | None
    ) -> StoredSession:
        def transform(current):
            return _require_record(current).model_copy(
                update={"operation_id": operation_id, "operation_phase": operation_phase}
            ), None

        return await self._mutate(token, transform)

    async def adjust_file_operations(self, token: LeaseToken, delta: int) -> StoredSession:
        if delta not in {-1, 1}:
            raise ValueError("file operation delta must be -1 or 1")

        def transform(current):
            record = _require_record(current)
            count = record.active_file_operations + delta
            if count < 0:
                raise RepositoryConflictError("file operation counter would become negative")
            changes: dict[str, Any] = {"active_file_operations": count}
            if delta == -1:
                changes["last_file_operation_at"] = self._now()
                if count == 0:
                    changes["idle_since"] = changes["last_file_operation_at"]
            return record.model_copy(update=changes), None

        return await self._mutate(token, transform)

    async def begin_file_operation(
        self,
        token: LeaseToken,
        *,
        operation_id: str,
        generation: int,
        executor_id: str,
        expires_at: datetime,
    ) -> StoredSession:
        return await self._change_file_operation(
            token,
            operation_id=operation_id,
            generation=generation,
            executor_id=executor_id,
            expires_at=expires_at,
        )

    async def finish_file_operation(
        self,
        token: LeaseToken,
        *,
        operation_id: str,
        generation: int,
        executor_id: str,
        confirmed: bool = True,
    ) -> StoredSession:
        return await self._change_file_operation(
            token,
            operation_id=operation_id,
            generation=generation,
            executor_id=executor_id,
            expires_at=None,
            confirmed=confirmed,
        )

    async def _change_file_operation(
        self,
        token: LeaseToken,
        *,
        operation_id: str,
        generation: int,
        executor_id: str,
        expires_at: datetime | None,
        confirmed: bool = True,
    ) -> StoredSession:
        record_ref, lease_ref = self._refs(token.key_hash)
        operation_ref = record_ref.collection("file_operations").document(
            hashlib.sha256(operation_id.encode()).hexdigest()
        )

        @async_transactional
        async def update(transaction):
            lease = await lease_ref.get(transaction=transaction)
            snapshot = await record_ref.get(transaction=transaction)
            operation_snapshot = await operation_ref.get(transaction=transaction)
            self._assert_lease(lease, token)
            if not snapshot.exists:
                raise RepositoryConflictError("Sandbox record is missing")
            current = decode_session(snapshot.to_dict() or {}, token.key_hash)
            operation = (
                FileOperation.model_validate(operation_snapshot.to_dict())
                if operation_snapshot.exists
                else None
            )
            record, operation, changed = change_file_operation(
                current.record,
                operation,
                operation_id=operation_id,
                executor_id=executor_id,
                generation=generation,
                now=self._now(),
                expires_at=expires_at,
                confirmed=confirmed,
            )
            if not changed:
                return current
            revision = int(current.resource_version) + 1
            transaction.set(record_ref, encode_session(record, revision))
            transaction.set(operation_ref, _native_dates(operation.model_dump(mode="json")))
            return StoredSession(record, str(revision))

        return await update(self.client.transaction())


def _require_record(current: StoredSession | None) -> SessionRecord:
    if current is None:
        raise RepositoryConflictError("Sandbox record is missing")
    return current.record


def firestore_repository_contract(repository: FirestoreSessionRepository) -> SessionRepository:
    return repository
