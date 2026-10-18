"""Storage operations required by Sandbox management and reconciliation."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from .repository import LeaseToken, StoredSession
from .types import CommandRecord, SessionEvent, SessionKey, SessionRecord


@runtime_checkable
class SessionRepository(Protocol):
    """Keep lease validation and record updates inside the storage adapter."""

    async def close(self) -> None: ...

    async def acquire_lease(self, key: SessionKey, holder_identity: str) -> LeaseToken: ...

    async def acquire_lease_by_hash(self, key_hash: str, holder_identity: str) -> LeaseToken: ...

    async def renew_lease(self, token: LeaseToken) -> LeaseToken: ...

    async def release_lease(self, token: LeaseToken) -> None: ...

    async def get(self, key: SessionKey) -> StoredSession | None: ...

    async def get_by_hash(self, key_hash: str) -> StoredSession | None: ...

    async def list_records(self) -> tuple[StoredSession, ...]: ...

    async def list_reconciliation_candidates(
        self, *, limit: int = 100, cursor: str | None = None
    ) -> tuple[tuple[StoredSession, ...], str | None]: ...

    async def create(self, token: LeaseToken, record: SessionRecord) -> StoredSession: ...

    async def compare_and_swap(
        self, token: LeaseToken, record: SessionRecord, *, expected_resource_version: str
    ) -> StoredSession: ...

    async def reserve_command(
        self, token: LeaseToken, *, generation: int, command_id: str, runtime_instance_id: str
    ) -> tuple[CommandRecord, bool]: ...

    async def append_event_once(self, token: LeaseToken, event: SessionEvent) -> bool: ...

    async def acknowledge_event(self, token: LeaseToken, event_id: str) -> bool: ...

    async def record_operation(
        self, token: LeaseToken, *, operation_id: str | None, operation_phase: str | None
    ) -> StoredSession: ...

    async def adjust_file_operations(self, token: LeaseToken, delta: int) -> StoredSession: ...

    async def begin_file_operation(
        self,
        token: LeaseToken,
        *,
        operation_id: str,
        generation: int,
        executor_id: str,
        expires_at: datetime,
    ) -> StoredSession: ...

    async def finish_file_operation(
        self,
        token: LeaseToken,
        *,
        operation_id: str,
        generation: int,
        executor_id: str,
        confirmed: bool = True,
    ) -> StoredSession: ...
