"""SQLite-backed session records intended for a durable, mounted volume."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .errors import (
    ConfigurationError,
    RepositoryCapacityError,
    RepositoryConflictError,
    SessionLeaseHeldError,
    SessionLeaseLostError,
    StaleGenerationError,
)
from .repository import LeaseToken, StoredSession, _serialize, _utc
from .types import CommandRecord, CommandState, SessionEvent, SessionKey, SessionRecord, utc_now


@dataclass(frozen=True, slots=True)
class _Lease:
    holder: str
    expires_at: datetime
    transitions: int


class SQLiteSessionRepository:
    """Persist session JSON and operation leases in one SQLite database.

    The database file should live on the API process's dedicated persistent
    volume. SQLite transactions protect concurrent requests and processes;
    the Kubernetes deployment should still run one API replica per RWO claim.
    """

    def __init__(
        self,
        database_path: str | Path,
        *,
        require_mount: bool = False,
        max_pending_events: int = 100,
        lease_duration_seconds: int = 60,
        clock=utc_now,
    ) -> None:
        if lease_duration_seconds <= 3:
            raise ValueError("lease_duration_seconds must be greater than 3")
        self.database_path = Path(database_path)
        if require_mount and not self.database_path.parent.is_mount():
            raise ConfigurationError(
                f"session repository directory {self.database_path.parent} is not a mounted volume"
            )
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock
        self._max_pending_events = max_pending_events
        self._lease_duration_seconds = lease_duration_seconds
        self._db = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA busy_timeout = 30000")
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = FULL")
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                key_hash TEXT PRIMARY KEY,
                record_json TEXT NOT NULL,
                version INTEGER NOT NULL CHECK(version > 0)
            );
            CREATE TABLE IF NOT EXISTS session_leases (
                key_hash TEXT PRIMARY KEY,
                holder TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                transitions INTEGER NOT NULL DEFAULT 0
            );
            """
        )

    async def close(self) -> None:
        self._db.close()

    async def acquire_lease(self, key: SessionKey, holder_identity: str) -> LeaseToken:
        return await self.acquire_lease_by_hash(key.stable_hash(), holder_identity)

    async def acquire_lease_by_hash(self, key_hash: str, holder_identity: str) -> LeaseToken:
        if not holder_identity.strip():
            raise ValueError("holder_identity must not be blank")
        _validate_key_hash(key_hash)
        now = _utc(self._clock())
        expires = now + timedelta(seconds=self._lease_duration_seconds)
        self._db.execute("BEGIN IMMEDIATE")
        try:
            row = self._db.execute("SELECT * FROM session_leases WHERE key_hash = ?", (key_hash,)).fetchone()
            if row is not None:
                lease = self._lease_from_row(row)
                if lease.holder and lease.holder != holder_identity and now < lease.expires_at:
                    raise SessionLeaseHeldError("session lease is held by another live worker")
                transitions = lease.transitions + int(bool(lease.holder and lease.holder != holder_identity))
                self._db.execute(
                    "UPDATE session_leases SET holder = ?, expires_at = ?, transitions = ? WHERE key_hash = ?",
                    (holder_identity, expires.isoformat(), transitions, key_hash),
                )
            else:
                self._db.execute(
                    "INSERT INTO session_leases(key_hash, holder, expires_at, transitions) VALUES (?, ?, ?, 0)",
                    (key_hash, holder_identity, expires.isoformat()),
                )
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        return LeaseToken(key_hash, holder_identity)

    async def renew_lease(self, token: LeaseToken) -> LeaseToken:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            lease = self._read_owned_lease(token)
            expires = _utc(self._clock()) + timedelta(seconds=self._lease_duration_seconds)
            self._db.execute(
                "UPDATE session_leases SET expires_at = ? WHERE key_hash = ? AND holder = ?",
                (expires.isoformat(), token.key_hash, token.holder_identity),
            )
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        return token

    async def release_lease(self, token: LeaseToken) -> None:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._read_owned_lease(token)
            self._db.execute(
                "UPDATE session_leases SET holder = '', expires_at = ? WHERE key_hash = ? AND holder = ?",
                (_utc(self._clock()).isoformat(), token.key_hash, token.holder_identity),
            )
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    async def get(self, key: SessionKey) -> StoredSession | None:
        return await self.get_by_hash(key.stable_hash())

    async def get_by_hash(self, key_hash: str) -> StoredSession | None:
        _validate_key_hash(key_hash)
        row = self._db.execute("SELECT record_json, version FROM sessions WHERE key_hash = ?", (key_hash,)).fetchone()
        if row is None:
            return None
        record = SessionRecord.model_validate_json(row["record_json"])
        if record.key_hash != key_hash:
            raise RepositoryConflictError("session record key hash does not match its database key")
        return StoredSession(record, str(row["version"]))

    async def list_records(self) -> tuple[StoredSession, ...]:
        rows = self._db.execute("SELECT record_json, version FROM sessions ORDER BY key_hash").fetchall()
        result = []
        for row in rows:
            record = SessionRecord.model_validate_json(row["record_json"])
            result.append(StoredSession(record, str(row["version"])))
        return tuple(result)

    async def create(self, token: LeaseToken, record: SessionRecord) -> StoredSession:
        self._validate_record(record)
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._assert_token(token, record.key_hash)
            self._db.execute(
                "INSERT INTO sessions(key_hash, record_json, version) VALUES (?, ?, 1)",
                (record.key_hash, _serialize(record)),
            )
            self._db.execute("COMMIT")
        except sqlite3.IntegrityError as exc:
            self._db.execute("ROLLBACK")
            raise RepositoryConflictError("session record already exists") from exc
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        return StoredSession(record, "1")

    async def compare_and_swap(
        self, token: LeaseToken, record: SessionRecord, *, expected_resource_version: str
    ) -> StoredSession:
        self._validate_record(record)
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._assert_token(token, record.key_hash)
            cursor = self._db.execute(
                "UPDATE sessions SET record_json = ?, version = version + 1 "
                "WHERE key_hash = ? AND version = ?",
                (_serialize(record), record.key_hash, int(expected_resource_version)),
            )
            if cursor.rowcount != 1:
                exists = self._db.execute("SELECT 1 FROM sessions WHERE key_hash = ?", (record.key_hash,)).fetchone()
                if exists is None:
                    raise RepositoryConflictError("session record disappeared before update")
                raise RepositoryConflictError("session database version changed")
            new_version = int(expected_resource_version) + 1
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        return StoredSession(record, str(new_version))

    async def reserve_command(
        self, token: LeaseToken, *, generation: int, command_id: str, runtime_instance_id: str
    ) -> tuple[CommandRecord, bool]:
        stored = await self.get_by_hash(token.key_hash)
        if stored is None:
            raise RepositoryConflictError("session record is missing during command reservation")
        if stored.record.generation != generation:
            raise StaleGenerationError("command handle refers to an older session generation")
        for command in stored.record.active_commands:
            if command.command_id == command_id:
                if command.generation != generation:
                    raise StaleGenerationError("command ID belongs to a different session generation")
                return command, False
        command = CommandRecord(
            command_id=command_id, generation=generation, runtime_instance_id=runtime_instance_id,
            state=CommandState.RUNNING, accepted_at=_utc(self._clock()),
        )
        record = stored.record.model_copy(update={"active_commands": (*stored.record.active_commands, command)})
        await self.compare_and_swap(token, record, expected_resource_version=stored.resource_version)
        return command, True

    async def append_event_once(self, token: LeaseToken, event: SessionEvent) -> bool:
        stored = await self.get_by_hash(token.key_hash)
        if stored is None:
            raise RepositoryConflictError("session record is missing while appending event")
        if any(item.event_id == event.event_id for item in stored.record.pending_events):
            return False
        record = stored.record.model_copy(update={"pending_events": (*stored.record.pending_events, event)})
        await self.compare_and_swap(token, record, expected_resource_version=stored.resource_version)
        return True

    async def acknowledge_event(self, token: LeaseToken, event_id: str) -> bool:
        stored = await self.get_by_hash(token.key_hash)
        if stored is None:
            raise RepositoryConflictError("session record is missing while acknowledging event")
        timestamp = _utc(self._clock())
        changed = False
        events = []
        for event in stored.record.pending_events:
            if event.event_id == event_id and event.acked_at is None:
                event = event.model_copy(update={"acked_at": timestamp})
                changed = True
            events.append(event)
        if not changed:
            return False
        record = stored.record.model_copy(update={"pending_events": tuple(events)})
        await self.compare_and_swap(token, record, expected_resource_version=stored.resource_version)
        return True

    async def record_operation(
        self, token: LeaseToken, *, operation_id: str | None, operation_phase: str | None
    ) -> StoredSession:
        stored = await self.get_by_hash(token.key_hash)
        if stored is None:
            raise RepositoryConflictError("session record is missing while recording operation")
        record = stored.record.model_copy(update={"operation_id": operation_id, "operation_phase": operation_phase})
        return await self.compare_and_swap(token, record, expected_resource_version=stored.resource_version)

    async def adjust_file_operations(self, token: LeaseToken, delta: int) -> StoredSession:
        if delta not in {-1, 1}:
            raise ValueError("file operation delta must be -1 or 1")
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._assert_token(token, token.key_hash)
            row = self._db.execute(
                "SELECT record_json, version FROM sessions WHERE key_hash = ?", (token.key_hash,)
            ).fetchone()
            if row is None:
                raise RepositoryConflictError("session record is missing while tracking file activity")
            record = SessionRecord.model_validate_json(row["record_json"])
            count = record.active_file_operations + delta
            if count < 0:
                raise RepositoryConflictError("file operation counter would become negative")
            changes = {"active_file_operations": count}
            if delta == -1:
                changes["last_file_operation_at"] = _utc(self._clock())
                if count == 0:
                    changes["idle_since"] = changes["last_file_operation_at"]
            updated = record.model_copy(update=changes)
            version = int(row["version"])
            self._db.execute(
                "UPDATE sessions SET record_json = ?, version = version + 1 WHERE key_hash = ? AND version = ?",
                (_serialize(updated), token.key_hash, version),
            )
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        return StoredSession(updated, str(version + 1))

    def _validate_record(self, record: SessionRecord) -> None:
        unacked_count = sum(event.acked_at is None for event in record.pending_events)
        if unacked_count > self._max_pending_events:
            raise RepositoryCapacityError("pending event count exceeds configured limit")
        _validate_key_hash(record.key_hash)

    def _assert_token(self, token: LeaseToken, key_hash: str) -> None:
        if token.key_hash != key_hash:
            raise SessionLeaseLostError("session lease token belongs to a different session")
        self._read_owned_lease(token)

    def _read_owned_lease(self, token: LeaseToken) -> _Lease:
        row = self._db.execute("SELECT * FROM session_leases WHERE key_hash = ?", (token.key_hash,)).fetchone()
        if row is None:
            raise SessionLeaseLostError("session lease no longer exists")
        lease = self._lease_from_row(row)
        if lease.holder != token.holder_identity or _utc(self._clock()) >= lease.expires_at:
            raise SessionLeaseLostError("session lease is no longer held by this worker")
        return lease

    @staticmethod
    def _lease_from_row(row: sqlite3.Row) -> _Lease:
        return _Lease(row["holder"], datetime.fromisoformat(row["expires_at"]), row["transitions"])


def _validate_key_hash(key_hash: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", key_hash) is None:
        raise ValueError("key_hash must be a lowercase SHA-256 hex string")
