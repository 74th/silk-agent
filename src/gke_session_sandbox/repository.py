"""ConfigMap-backed session records with per-session Kubernetes Lease fencing."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from .errors import RepositoryCapacityError, RepositoryConflictError, SessionLeaseHeldError, SessionLeaseLostError, StaleGenerationError
from .types import CommandRecord, CommandState, SessionEvent, SessionKey, SessionRecord, utc_now

_RECORD_DATA_KEY = "record.json"
_KEY_HASH_ANNOTATION = "session-sandbox.example.com/session-key-hash"


class CoreV1Api(Protocol):
    async def read_namespaced_config_map(self, name: str, namespace: str) -> Any: ...
    async def create_namespaced_config_map(self, namespace: str, body: Any) -> Any: ...
    async def patch_namespaced_config_map(self, name: str, namespace: str, body: Any) -> Any: ...
    async def list_namespaced_config_map(self, namespace: str, label_selector: str | None = None) -> Any: ...


class CoordinationV1Api(Protocol):
    async def read_namespaced_lease(self, name: str, namespace: str) -> Any: ...
    async def create_namespaced_lease(self, namespace: str, body: Any) -> Any: ...
    async def patch_namespaced_lease(self, name: str, namespace: str, body: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class LeaseToken:
    key_hash: str
    holder_identity: str


@dataclass(frozen=True, slots=True)
class StoredSession:
    record: SessionRecord
    resource_version: str


class ConfigMapSessionRepository:
    """Persist one strict JSON SessionRecord in a ConfigMap per session key."""

    def __init__(
        self,
        core_api: CoreV1Api,
        coordination_api: CoordinationV1Api,
        *,
        namespace: str,
        record_limit_bytes: int = 900 * 1024,
        max_pending_events: int = 100,
        lease_duration_seconds: int = 30,
        clock=utc_now,
    ) -> None:
        self._core = core_api
        self._coordination = coordination_api
        self._namespace = namespace
        self._record_limit_bytes = record_limit_bytes
        self._max_pending_events = max_pending_events
        self._lease_duration_seconds = lease_duration_seconds
        self._clock = clock

    async def acquire_lease(self, key: SessionKey, holder_identity: str) -> LeaseToken:
        if not holder_identity.strip():
            raise ValueError("holder_identity must not be blank")
        key_hash = key.stable_hash()
        name = _resource_name("lease", key_hash)
        now = _utc(self._clock())
        try:
            lease = await self._coordination.read_namespaced_lease(name, self._namespace)
        except Exception as exc:
            if _status(exc) != 404:
                raise
            body = _lease_body(name, self._namespace, holder_identity, now, self._lease_duration_seconds)
            try:
                await self._coordination.create_namespaced_lease(self._namespace, body)
            except Exception as create_exc:
                if _status(create_exc) == 409:
                    raise SessionLeaseHeldError("session lease was acquired concurrently") from create_exc
                raise
            return LeaseToken(key_hash, holder_identity)

        spec = _field(lease, "spec", {})
        current_holder = _field(spec, "holderIdentity")
        if current_holder and current_holder != holder_identity and not _lease_expired(spec, now):
            raise SessionLeaseHeldError("session lease is held by another live worker")
        transitions = _field(spec, "leaseTransitions", 0) or 0
        if current_holder and current_holder != holder_identity:
            transitions += 1
        resource_version = _resource_version(lease)
        body = [
            {"op": "test", "path": "/metadata/resourceVersion", "value": resource_version},
            {"op": "replace", "path": "/spec", "value": _lease_spec(holder_identity, now, self._lease_duration_seconds, transitions)},
        ]
        try:
            await self._coordination.patch_namespaced_lease(name, self._namespace, body)
        except Exception as exc:
            if _status(exc) == 409:
                raise SessionLeaseHeldError("session lease changed while acquiring it") from exc
            raise
        return LeaseToken(key_hash, holder_identity)

    async def renew_lease(self, token: LeaseToken) -> LeaseToken:
        lease = await self._read_owned_lease(token)
        spec = _field(lease, "spec", {})
        resource_version = _resource_version(lease)
        body = [
            {"op": "test", "path": "/metadata/resourceVersion", "value": resource_version},
            {"op": "replace", "path": "/spec", "value": _lease_spec(
                token.holder_identity, _utc(self._clock()), self._lease_duration_seconds,
                _field(spec, "leaseTransitions", 0) or 0,
            )},
        ]
        try:
            await self._coordination.patch_namespaced_lease(_resource_name("lease", token.key_hash), self._namespace, body)
        except Exception as exc:
            if _status(exc) == 409:
                raise SessionLeaseLostError("session lease was lost during renewal") from exc
            raise
        return token

    async def release_lease(self, token: LeaseToken) -> None:
        lease = await self._read_owned_lease(token)
        spec = _field(lease, "spec", {})
        resource_version = _resource_version(lease)
        body = [
            {"op": "test", "path": "/metadata/resourceVersion", "value": resource_version},
            {"op": "replace", "path": "/spec", "value": _lease_spec(
                "", _utc(self._clock()), self._lease_duration_seconds,
                _field(spec, "leaseTransitions", 0) or 0,
            )},
        ]
        try:
            await self._coordination.patch_namespaced_lease(_resource_name("lease", token.key_hash), self._namespace, body)
        except Exception as exc:
            if _status(exc) == 409:
                raise SessionLeaseLostError("session lease changed before release") from exc
            raise

    async def get(self, key: SessionKey) -> StoredSession | None:
        return await self.get_by_hash(key.stable_hash())

    async def list_records(self) -> tuple[StoredSession, ...]:
        response = await self._core.list_namespaced_config_map(
            self._namespace, label_selector="session-sandbox.example.com/managed=true"
        )
        stored: list[StoredSession] = []
        for config_map in _field(response, "items", []) or []:
            metadata = _field(config_map, "metadata", {})
            annotations = _field(metadata, "annotations", {}) or {}
            key_hash = annotations.get(_KEY_HASH_ANNOTATION)
            if not isinstance(key_hash, str):
                continue
            record = await self.get_by_hash(key_hash)
            if record is not None:
                stored.append(record)
        return tuple(sorted(stored, key=lambda item: (item.record.key_hash, item.record.generation)))

    async def create(self, token: LeaseToken, record: SessionRecord) -> StoredSession:
        await self._assert_token(token, record.key_hash)
        self._validate_record(record)
        name = _resource_name("session", token.key_hash)
        body = {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {
                "name": name, "namespace": self._namespace,
                "labels": {"session-sandbox.example.com/managed": "true"},
                "annotations": {_KEY_HASH_ANNOTATION: token.key_hash},
            },
            "data": {_RECORD_DATA_KEY: _serialize(record)},
        }
        try:
            created = await self._core.create_namespaced_config_map(self._namespace, body)
        except Exception as exc:
            if _status(exc) == 409:
                raise RepositoryConflictError("session ConfigMap already exists") from exc
            raise
        return StoredSession(record, _resource_version(created))

    async def compare_and_swap(
        self, token: LeaseToken, record: SessionRecord, *, expected_resource_version: str
    ) -> StoredSession:
        await self._assert_token(token, record.key_hash)
        self._validate_record(record)
        existing = await self.get_by_hash(token.key_hash)
        if existing is None:
            raise RepositoryConflictError("session ConfigMap disappeared before update")
        if existing.resource_version != expected_resource_version:
            raise RepositoryConflictError("session ConfigMap resourceVersion changed")
        name = _resource_name("session", token.key_hash)
        body = [
            {"op": "test", "path": "/metadata/resourceVersion", "value": expected_resource_version},
            {"op": "replace", "path": f"/data/{_RECORD_DATA_KEY}", "value": _serialize(record)},
        ]
        try:
            updated = await self._core.patch_namespaced_config_map(name, self._namespace, body)
        except Exception as exc:
            if _status(exc) == 409:
                raise RepositoryConflictError("session ConfigMap changed during compare-and-swap") from exc
            raise
        return StoredSession(record, _resource_version(updated))

    async def reserve_command(
        self, token: LeaseToken, *, generation: int, command_id: str, runtime_instance_id: str
    ) -> tuple[CommandRecord, bool]:
        """Persist command admission before contacting the runtime; return (record, created)."""
        stored = await self.get_by_hash(token.key_hash)
        if stored is None:
            raise RepositoryConflictError("session ConfigMap is missing during command reservation")
        if stored.record.generation != generation:
            raise StaleGenerationError("command handle refers to an older session generation")
        for command in stored.record.active_commands:
            if command.command_id == command_id:
                if command.generation != generation:
                    raise StaleGenerationError("command ID belongs to a different session generation")
                return command, False
        command = CommandRecord(
            command_id=command_id,
            generation=generation,
            runtime_instance_id=runtime_instance_id,
            state=CommandState.RUNNING,
            accepted_at=_utc(self._clock()),
        )
        record = stored.record.model_copy(update={"active_commands": (*stored.record.active_commands, command)})
        await self.compare_and_swap(token, record, expected_resource_version=stored.resource_version)
        return command, True

    async def append_event_once(self, token: LeaseToken, event: SessionEvent) -> bool:
        """Append a durable event once by event_id; return False for a duplicate."""
        stored = await self.get_by_hash(token.key_hash)
        if stored is None:
            raise RepositoryConflictError("session ConfigMap is missing while appending event")
        if any(item.event_id == event.event_id for item in stored.record.pending_events):
            return False
        record = stored.record.model_copy(update={"pending_events": (*stored.record.pending_events, event)})
        await self.compare_and_swap(token, record, expected_resource_version=stored.resource_version)
        return True

    async def acknowledge_event(self, token: LeaseToken, event_id: str) -> bool:
        """Persist event acknowledgment; return False if event is absent or already acked."""
        stored = await self.get_by_hash(token.key_hash)
        if stored is None:
            raise RepositoryConflictError("session ConfigMap is missing while acknowledging event")
        found = False
        changed = False
        timestamp = _utc(self._clock())
        events = []
        for event in stored.record.pending_events:
            if event.event_id == event_id:
                found = True
                if event.acked_at is None:
                    event = event.model_copy(update={"acked_at": timestamp})
                    changed = True
            events.append(event)
        if not found or not changed:
            return False
        record = stored.record.model_copy(update={"pending_events": tuple(events)})
        await self.compare_and_swap(token, record, expected_resource_version=stored.resource_version)
        return True

    async def record_operation(
        self, token: LeaseToken, *, operation_id: str | None, operation_phase: str | None
    ) -> StoredSession:
        """Persist an operation intent or phase so a worker can resume after a crash."""
        stored = await self.get_by_hash(token.key_hash)
        if stored is None:
            raise RepositoryConflictError("session ConfigMap is missing while recording operation")
        record = stored.record.model_copy(update={"operation_id": operation_id, "operation_phase": operation_phase})
        return await self.compare_and_swap(token, record, expected_resource_version=stored.resource_version)

    async def get_by_hash(self, key_hash: str) -> StoredSession | None:
        if re.fullmatch(r"[0-9a-f]{64}", key_hash) is None:
            raise ValueError("key_hash must be a lowercase SHA-256 hex string")
        try:
            cm = await self._core.read_namespaced_config_map(_resource_name("session", key_hash), self._namespace)
        except Exception as exc:
            if _status(exc) == 404:
                return None
            raise
        metadata = _field(cm, "metadata", {})
        annotations = _field(metadata, "annotations", {}) or {}
        if annotations.get(_KEY_HASH_ANNOTATION) != key_hash:
            raise RepositoryConflictError("session ConfigMap key hash does not match its name")
        raw = (_field(cm, "data", {}) or {}).get(_RECORD_DATA_KEY)
        if not isinstance(raw, str):
            raise RepositoryConflictError("session ConfigMap is missing its record payload")
        record = SessionRecord.model_validate_json(raw)
        if record.key_hash != key_hash:
            raise RepositoryConflictError("session record key hash does not match its ConfigMap")
        return StoredSession(record, _resource_version(cm))

    def _validate_record(self, record: SessionRecord) -> None:
        unacked_count = sum(event.acked_at is None for event in record.pending_events)
        if unacked_count > self._max_pending_events:
            raise RepositoryCapacityError("pending event count exceeds configured limit")
        if len(_serialize(record).encode("utf-8")) > self._record_limit_bytes:
            raise RepositoryCapacityError("session record exceeds configured ConfigMap size limit")

    async def _assert_token(self, token: LeaseToken, key_hash: str) -> None:
        if token.key_hash != key_hash:
            raise SessionLeaseLostError("session lease token belongs to a different session")
        await self._read_owned_lease(token)

    async def _read_owned_lease(self, token: LeaseToken) -> Any:
        try:
            lease = await self._coordination.read_namespaced_lease(_resource_name("lease", token.key_hash), self._namespace)
        except Exception as exc:
            if _status(exc) == 404:
                raise SessionLeaseLostError("session lease no longer exists") from exc
            raise
        spec = _field(lease, "spec", {})
        if _field(spec, "holderIdentity") != token.holder_identity or _lease_expired(spec, _utc(self._clock())):
            raise SessionLeaseLostError("session lease is no longer held by this worker")
        return lease


def _resource_name(prefix: str, key_hash: str) -> str:
    return f"{prefix}-{key_hash[:52]}"


def _serialize(record: SessionRecord) -> str:
    return record.model_dump_json(exclude_none=False)


def _lease_body(name: str, namespace: str, holder: str, now: datetime, duration: int) -> dict[str, Any]:
    return {
        "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
        "metadata": {"name": name, "namespace": namespace},
        "spec": _lease_spec(holder, now, duration, 0),
    }


def _lease_spec(holder: str, now: datetime, duration: int, transitions: int) -> dict[str, Any]:
    timestamp = now.isoformat().replace("+00:00", "Z")
    return {
        "holderIdentity": holder, "leaseDurationSeconds": duration,
        "acquireTime": timestamp, "renewTime": timestamp, "leaseTransitions": transitions,
    }


def _lease_expired(spec: Any, now: datetime) -> bool:
    holder = _field(spec, "holderIdentity")
    if not holder:
        return True
    renewed = _field(spec, "renewTime") or _field(spec, "acquireTime")
    duration = _field(spec, "leaseDurationSeconds")
    if not renewed or not duration:
        return True
    if isinstance(renewed, str):
        renewed = datetime.fromisoformat(renewed.replace("Z", "+00:00"))
    return now >= renewed + timedelta(seconds=duration)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower(), default)


def _resource_version(resource: Any) -> str:
    version = _field(_field(resource, "metadata", {}), "resourceVersion")
    if not version:
        raise RepositoryConflictError("Kubernetes resource is missing resourceVersion")
    return str(version)


def _status(exc: Exception) -> int | None:
    return getattr(exc, "status", None)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(timezone.utc)
