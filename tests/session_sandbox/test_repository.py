from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from gke_session_sandbox import (
    ConfigMapSessionRepository,
    CommandState,
    EventType,
    RepositoryCapacityError,
    RepositoryConflictError,
    SessionEvent,
    SessionKey,
    SessionLeaseHeldError,
    SessionLeaseLostError,
    SessionRecord,
    SessionState,
)


class ApiError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status


class FakeKubernetes:
    def __init__(self) -> None:
        self.config_maps: dict[tuple[str, str], dict] = {}
        self.leases: dict[tuple[str, str], dict] = {}
        self._version = 0

    def _next_rv(self) -> str:
        self._version += 1
        return str(self._version)

    async def read_namespaced_config_map(self, name: str, namespace: str) -> dict:
        try:
            return deepcopy(self.config_maps[(namespace, name)])
        except KeyError as exc:
            raise ApiError(404) from exc

    async def create_namespaced_config_map(self, namespace: str, body: dict) -> dict:
        key = namespace, body["metadata"]["name"]
        if key in self.config_maps:
            raise ApiError(409)
        stored = deepcopy(body)
        stored["metadata"]["resourceVersion"] = self._next_rv()
        self.config_maps[key] = stored
        return deepcopy(stored)

    async def patch_namespaced_config_map(self, name: str, namespace: str, body: dict) -> dict:
        key = namespace, name
        current = self.config_maps.get(key)
        if current is None:
            raise ApiError(404)
        if body[0]["value"] != current["metadata"]["resourceVersion"]:
            raise ApiError(409)
        stored = deepcopy(current)
        stored["data"]["record.json"] = body[1]["value"]
        stored["metadata"]["resourceVersion"] = self._next_rv()
        self.config_maps[key] = stored
        return deepcopy(stored)

    async def read_namespaced_lease(self, name: str, namespace: str) -> dict:
        try:
            return deepcopy(self.leases[(namespace, name)])
        except KeyError as exc:
            raise ApiError(404) from exc

    async def create_namespaced_lease(self, namespace: str, body: dict) -> dict:
        key = namespace, body["metadata"]["name"]
        if key in self.leases:
            raise ApiError(409)
        stored = deepcopy(body)
        stored["metadata"]["resourceVersion"] = self._next_rv()
        self.leases[key] = stored
        return deepcopy(stored)

    async def patch_namespaced_lease(self, name: str, namespace: str, body: dict) -> dict:
        key = namespace, name
        current = self.leases.get(key)
        if current is None:
            raise ApiError(404)
        if body[0]["value"] != current["metadata"]["resourceVersion"]:
            raise ApiError(409)
        stored = deepcopy(current)
        stored["spec"] = deepcopy(body[1]["value"])
        stored["metadata"]["resourceVersion"] = self._next_rv()
        self.leases[key] = stored
        return deepcopy(stored)


def record_for(key: SessionKey, **changes: object) -> SessionRecord:
    return SessionRecord(key_hash=key.stable_hash(), generation=1, state=SessionState.CREATING, **changes)


@pytest.mark.asyncio
async def test_repository_round_trip_and_resource_version_compare_and_swap() -> None:
    api = FakeKubernetes()
    repo = ConfigMapSessionRepository(api, api, namespace="control")
    key = SessionKey(tenant_id="t", session_id="s")
    token = await repo.acquire_lease(key, "worker-a")
    created = await repo.create(token, record_for(key))

    stored = await repo.get(key)
    assert stored == created
    updated_record = stored.record.model_copy(update={"state": SessionState.READY})
    updated = await repo.compare_and_swap(token, updated_record, expected_resource_version=stored.resource_version)
    assert updated.record.state is SessionState.READY
    assert updated.resource_version != stored.resource_version
    with pytest.raises(RepositoryConflictError, match="resourceVersion changed"):
        await repo.compare_and_swap(token, record_for(key), expected_resource_version=stored.resource_version)


@pytest.mark.asyncio
async def test_lease_excludes_other_worker_and_lost_lease_blocks_writes() -> None:
    api = FakeKubernetes()
    repo = ConfigMapSessionRepository(api, api, namespace="control")
    key = SessionKey(tenant_id="t", session_id="s")
    token = await repo.acquire_lease(key, "worker-a")
    with pytest.raises(SessionLeaseHeldError):
        await repo.acquire_lease(key, "worker-b")
    lease = api.leases[("control", f"lease-{key.stable_hash()[:52]}")]
    lease["spec"]["holderIdentity"] = "worker-b"
    with pytest.raises(SessionLeaseLostError):
        await repo.create(token, record_for(key))


@pytest.mark.asyncio
async def test_expired_lease_can_be_taken_over_and_transition_count_increments() -> None:
    now = [datetime(2030, 1, 1, tzinfo=timezone.utc)]
    api = FakeKubernetes()
    repo = ConfigMapSessionRepository(api, api, namespace="control", clock=lambda: now[0], lease_duration_seconds=10)
    key = SessionKey(tenant_id="t", session_id="s")
    await repo.acquire_lease(key, "worker-a")
    now[0] += timedelta(seconds=11)
    token = await repo.acquire_lease(key, "worker-b")
    lease = api.leases[("control", f"lease-{key.stable_hash()[:52]}")]
    assert lease["spec"]["holderIdentity"] == token.holder_identity
    assert lease["spec"]["leaseTransitions"] == 1


@pytest.mark.asyncio
async def test_repository_enforces_record_and_pending_event_capacity() -> None:
    api = FakeKubernetes()
    key = SessionKey(tenant_id="t", session_id="s")
    clock = datetime(2030, 1, 1, tzinfo=timezone.utc)
    repo = ConfigMapSessionRepository(api, api, namespace="control", max_pending_events=1)
    token = await repo.acquire_lease(key, "worker")
    event = SessionEvent(
        event_id="1", generation=1, event_type=EventType.WORKSPACE_LOST,
        occurred_at=clock, reason="lost", context_message="lost",
    )
    other_event = event.model_copy(update={"event_id": "2"})
    with pytest.raises(RepositoryCapacityError, match="pending event count"):
        await repo.create(token, record_for(key, pending_events=(event, other_event)))
    acked = event.model_copy(update={"acked_at": clock})
    assert await repo.create(token, record_for(key, pending_events=(acked, event)))
    small = ConfigMapSessionRepository(api, api, namespace="control", record_limit_bytes=20)
    with pytest.raises(RepositoryCapacityError, match="record exceeds"):
        await small.create(token, record_for(key))


@pytest.mark.asyncio
async def test_missing_record_is_not_treated_as_a_successful_update() -> None:
    api = FakeKubernetes()
    repo = ConfigMapSessionRepository(api, api, namespace="control")
    key = SessionKey(tenant_id="t", session_id="s")
    assert await repo.get(key) is None
    token = await repo.acquire_lease(key, "worker")
    with pytest.raises(RepositoryConflictError, match="disappeared"):
        await repo.compare_and_swap(token, record_for(key), expected_resource_version="1")


@pytest.mark.asyncio
async def test_api_failure_is_not_misreported_as_not_found() -> None:
    api = FakeKubernetes()
    repo = ConfigMapSessionRepository(api, api, namespace="control")

    async def unavailable(name: str, namespace: str) -> dict:
        raise ApiError(503)

    api.read_namespaced_config_map = unavailable
    with pytest.raises(ApiError) as exc_info:
        await repo.get(SessionKey(tenant_id="t", session_id="s"))
    assert exc_info.value.status == 503


@pytest.mark.asyncio
async def test_release_and_renew_require_current_holder() -> None:
    api = FakeKubernetes()
    repo = ConfigMapSessionRepository(api, api, namespace="control")
    key = SessionKey(tenant_id="t", session_id="s")
    token = await repo.acquire_lease(key, "worker")
    assert await repo.renew_lease(token) == token
    await repo.release_lease(token)
    with pytest.raises(SessionLeaseLostError):
        await repo.renew_lease(token)


@pytest.mark.asyncio
async def test_command_reservation_and_operation_phase_survive_repository_reload() -> None:
    api = FakeKubernetes()
    repo = ConfigMapSessionRepository(api, api, namespace="control")
    key = SessionKey(tenant_id="t", session_id="s")
    token = await repo.acquire_lease(key, "worker")
    await repo.create(token, record_for(key, operation_id="create-1", operation_phase="sandbox-created"))

    command, created = await repo.reserve_command(
        token, generation=1, command_id="cmd-1", runtime_instance_id="runtime-1"
    )
    same_command, duplicate_created = await repo.reserve_command(
        token, generation=1, command_id="cmd-1", runtime_instance_id="runtime-1"
    )
    stored = await repo.get(key)

    assert created is True
    assert duplicate_created is False
    assert same_command == command
    assert command.state is CommandState.RUNNING
    assert command.started_at is None
    assert stored.record.operation_phase == "sandbox-created"
    assert [item.command_id for item in stored.record.active_commands] == ["cmd-1"]


@pytest.mark.asyncio
async def test_append_event_once_and_ack_are_durable_and_idempotent() -> None:
    api = FakeKubernetes()
    repo = ConfigMapSessionRepository(api, api, namespace="control")
    key = SessionKey(tenant_id="t", session_id="s")
    token = await repo.acquire_lease(key, "worker")
    await repo.create(token, record_for(key))
    event = SessionEvent(
        event_id="evt-1", generation=1, event_type=EventType.WORKSPACE_CLEARED,
        occurred_at=datetime(2030, 1, 1, tzinfo=timezone.utc), reason="expired",
        context_message="Workspace was cleared.",
    )

    assert await repo.append_event_once(token, event) is True
    assert await repo.append_event_once(token, event) is False
    assert await repo.acknowledge_event(token, "evt-1") is True
    assert await repo.acknowledge_event(token, "evt-1") is False
    stored = await repo.get(key)
    assert stored.record.pending_events[0].acked_at is not None
