from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from gke_session_sandbox import (
    CommandRecord,
    CommandState,
    CoreSandboxBackend,
    ManagerConfig,
    ResourceRef,
    ResourceOwnershipError,
    SessionConfigurationConflictError,
    SessionKey,
    SessionLeaseHeldError,
    SessionRecord,
    SessionState,
    SandboxManager,
    StoredSession,
    StaleGenerationError,
)


@pytest.mark.asyncio
async def test_tracker_gate_retries_transient_server_failure() -> None:
    attempts = 0

    async def set_gate(_request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return web.Response(status=503)
        return web.json_response({"active_commands": 0})

    app = web.Application()
    app.router.add_put("/v1/commands/gate", set_gate)
    server = TestServer(app)
    await server.start_server()
    try:
        active = await SandboxManager._set_tracker_gate(str(server.make_url("")), True)
    finally:
        await server.close()

    assert active == 0
    assert attempts == 2


class MemoryRepository:
    def __init__(self) -> None:
        self.records: dict[str, StoredSession] = {}
        self.holders: dict[str, str] = {}
        self.version = 0
        self.deleted = False

    async def acquire_lease(self, key: SessionKey, holder: str):
        return await self.acquire_lease_by_hash(key.stable_hash(), holder)

    async def acquire_lease_by_hash(self, key_hash: str, holder: str):
        if key_hash in self.holders and self.holders[key_hash] != holder:
            raise SessionLeaseHeldError()
        self.holders[key_hash] = holder
        return type("Token", (), {"key_hash": key_hash, "holder_identity": holder})()

    async def release_lease(self, token) -> None:
        self.holders.pop(token.key_hash, None)

    async def get(self, key: SessionKey):
        return self.records.get(key.stable_hash())

    async def create(self, token, record: SessionRecord) -> StoredSession:
        if record.key_hash in self.records:
            raise RuntimeError("duplicate record")
        self.version += 1
        stored = StoredSession(record, str(self.version))
        self.records[record.key_hash] = stored
        return stored

    async def compare_and_swap(self, token, record: SessionRecord, *, expected_resource_version: str) -> StoredSession:
        current = self.records[record.key_hash]
        if current.resource_version != expected_resource_version:
            raise RuntimeError("CAS conflict")
        self.version += 1
        stored = StoredSession(record, str(self.version))
        self.records[record.key_hash] = stored
        return stored

    async def list_records(self):
        return tuple(self.records.values())

    async def reserve_command(self, token, *, generation: int, command_id: str, runtime_instance_id: str):
        stored = self.records[token.key_hash]
        record = stored.record
        for command in record.active_commands:
            if command.command_id == command_id:
                return command, False
        command = CommandRecord(
            command_id=command_id, generation=generation, runtime_instance_id=runtime_instance_id,
            state=CommandState.RUNNING, accepted_at=datetime.now(timezone.utc),
        )
        updated = record.model_copy(update={"active_commands": (*record.active_commands, command)})
        result = await self.compare_and_swap(token, updated, expected_resource_version=stored.resource_version)
        return command, True

    async def adjust_file_operations(self, token, delta: int):
        stored = self.records[token.key_hash]
        record = stored.record.model_copy(update={
            "active_file_operations": stored.record.active_file_operations + delta,
        })
        return await self.compare_and_swap(token, record, expected_resource_version=stored.resource_version)


class FakeBackend:
    def __init__(self) -> None:
        self.created: list[tuple[str, dict, tuple[dict, ...]]] = []
        self.deleted: list[str] = []
        self.suspended: list[str] = []
        self.started: list[str] = []
        self.api_client = None
        self.endpoint = ""
        self.endpoint_refs: list[ResourceRef] = []

    async def create_with_policies(self, manifest, policies, *, namespace: str, name: str):
        if getattr(self, "fail_create", False):
            raise RuntimeError("simulated create failure")
        self.created.append((name, deepcopy(dict(manifest)), tuple(deepcopy(dict(x)) for x in policies)))
        ref = ResourceRef(api_version="agents.x-k8s.io/v1beta1", kind="Sandbox", namespace=namespace, name=name, uid=f"uid-{name}")
        return ref, ()

    async def wait_ready(self, ref, **kwargs):
        return {"metadata": {"uid": ref.uid}, "status": {"podUID": f"pod-{ref.uid}"}}

    async def start(self, ref):
        self.started.append(ref.name)
        return ref

    async def resume(self, ref):
        self.started.append(ref.name)
        return ref

    async def set_expiration(self, ref, expires_at):
        self.expiration = expires_at

    async def patch_annotations(self, ref, annotations):
        self.annotations = dict(annotations)

    async def suspend(self, ref):
        self.suspended.append(ref.name)

    async def wait_suspended(self, ref, **kwargs):
        return {"metadata": {"uid": ref.uid}}

    async def delete(self, ref):
        self.deleted.append(ref.name)

    async def wait_deleted(self, ref, **kwargs):
        return None

    async def get_owned(self, ref):
        return {"metadata": {"uid": ref.uid}}

    async def runtime_endpoint(self, ref, **kwargs):
        self.endpoint_refs.append(ref)
        return self.endpoint


def config() -> ManagerConfig:
    return ManagerConfig(
        runtime={"image": "example.invalid/sandbox:v1"},
        environment_fingerprint_key="a-stable-secret-key-with-at-least-32-bytes",
    )


def template() -> dict:
    return {
        "apiVersion": "agents.x-k8s.io/v1beta1",
        "kind": "Sandbox",
        "metadata": {"labels": {"app.kubernetes.io/name": "test"}},
        "spec": {"service": True, "podTemplate": {"spec": {"containers": [
            {"name": "sandboxd", "image": "example.invalid/sandbox:v1", "env": [{"name": "USER_EMAIL", "value": "default@example.com"}]}
        ]}}},
    }


@pytest.mark.asyncio
async def test_ensure_is_idempotent_env_checked_and_distinct_keys_are_isolated() -> None:
    repository, backend = MemoryRepository(), FakeBackend()
    manager = SandboxManager(config(), repository, backend, template=template())
    first_key = SessionKey(tenant_id="tenant", session_id="one")
    second_key = SessionKey(tenant_id="tenant", session_id="two")

    first = await manager.ensure(first_key, env={"USER_EMAIL": "one@example.com"})
    again = await manager.ensure(first_key, env={"USER_EMAIL": "one@example.com"})
    second = await manager.ensure(second_key, env={"USER_EMAIL": "two@example.com"})

    assert first.generation == again.generation == 1
    assert first.sandbox_ref == again.sandbox_ref
    assert first.sandbox_ref.name != second.sandbox_ref.name
    assert len(backend.created) == 2
    effective_env = backend.created[0][1]["spec"]["podTemplate"]["spec"]["containers"][0]["env"]
    assert effective_env[0] == {"name": "USER_EMAIL", "value": "one@example.com"}
    assert {entry["name"]: entry["value"] for entry in effective_env[1:]} == {
        "SESSION_SANDBOX_COMMAND_OUTPUT_LIMIT_BYTES": str(config().lifecycle.command_output_limit_bytes),
        "SESSION_SANDBOX_SESSION_OUTPUT_LIMIT_BYTES": str(config().lifecycle.session_output_limit_bytes),
        "SESSION_SANDBOX_BUFFERED_RESPONSE_LIMIT_BYTES": str(config().lifecycle.buffered_response_limit_bytes),
        "SESSION_SANDBOX_OUTPUT_RETENTION_SECONDS": str(config().lifecycle.retention_seconds),
    }
    with pytest.raises(SessionConfigurationConflictError):
        await manager.ensure(first_key, env={"USER_EMAIL": "changed@example.com"})


@pytest.mark.asyncio
async def test_simultaneous_ensure_for_same_key_uses_one_sandbox() -> None:
    repository, backend = MemoryRepository(), FakeBackend()
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="concurrent")

    first, second = await pytest.importorskip("asyncio").gather(manager.ensure(key), manager.ensure(key))

    assert first.sandbox_ref == second.sandbox_ref
    assert len(backend.created) == 1


@pytest.mark.asyncio
async def test_session_command_client_ensures_session_and_resolves_current_endpoint() -> None:
    received = []

    async def create(request):
        received.append(await request.json())
        return web.json_response({"command_id": received[-1]["command_id"], "state": "running"}, status=202)

    async def command_state(_request):
        return web.json_response({
            "command_id": "client-command", "state": "finished", "exit_code": 0,
            "started_at": "2030-01-01T00:00:00Z", "finished_at": "2030-01-01T00:00:01Z",
        })

    app = web.Application()
    app.router.add_post("/v1/commands", create)
    app.router.add_get("/v1/commands/{command_id}", command_state)
    async with TestServer(app) as server:
        repository, backend = MemoryRepository(), FakeBackend()
        backend.endpoint = str(server.make_url("")).rstrip("/")
        manager = SandboxManager(config(), repository, backend, template=template())
        handle = await manager.ensure(SessionKey(tenant_id="tenant", session_id="commands"))
        async with handle.commands as commands:
            accepted = await commands.start(["printf", "ok"], command_id="client-command")

        assert accepted["command_id"] == "client-command"
        assert received[0]["command"] == ["printf", "ok"]
        assert backend.endpoint_refs == [handle.sandbox_ref]
        stored = await repository.get(handle.key)
        assert stored.record.active_commands[0].command_id == "client-command"
        await commands.get("client-command")
        stored = await repository.get(handle.key)
        assert stored.record.active_commands == ()
        assert stored.record.last_command_finished_at.isoformat().startswith("2030-01-01")
        await manager.close()


@pytest.mark.asyncio
async def test_suspend_resume_and_manager_close_do_not_delete_session() -> None:
    async def gate(request):
        body = await request.json()
        return web.json_response({"accepting": body["accepting"], "active_commands": 0})

    app = web.Application()
    app.router.add_put("/v1/commands/gate", gate)
    server = TestServer(app)
    await server.start_server()
    repository, backend = MemoryRepository(), FakeBackend()
    backend.endpoint = str(server.make_url("/" )).rstrip("/")
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="thread")
    handle = await manager.ensure(key)
    suspended = await manager.suspend(handle)
    resumed = await manager.resume(suspended)
    await manager.close()

    assert suspended.state is SessionState.SUSPENDED
    assert resumed.state is SessionState.READY
    assert backend.suspended == [handle.sandbox_ref.name]
    assert backend.started == [handle.sandbox_ref.name]
    assert backend.deleted == []
    assert repository.records[key.stable_hash()].record.state is SessionState.READY
    await server.close()


@pytest.mark.asyncio
async def test_ensure_after_retention_deadline_cleans_old_generation_before_creating_new_one() -> None:
    repository, backend = MemoryRepository(), FakeBackend()
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="expired")
    old = await manager.ensure(key)
    token = await repository.acquire_lease(key, "expire-setup")
    stored = await repository.get(key)
    expired = stored.record.model_copy(update={
        "state": SessionState.SUSPENDED,
        "expires_at": datetime(2020, 1, 1, tzinfo=timezone.utc),
    })
    await repository.compare_and_swap(token, expired, expected_resource_version=stored.resource_version)
    await repository.release_lease(token)

    new = await manager.ensure(key)

    assert old.generation == 1
    assert new.generation == 2
    assert new.sandbox_ref.name != old.sandbox_ref.name
    assert backend.deleted == [old.sandbox_ref.name]
    assert len(new.pending_context_events) == 1
    assert new.pending_context_events[0].event_type.value == "workspace_cleared"
    assert new.pending_context_events[0].reason == "retention-expired"
    assert "previous workspace was cleared" in new.pending_context_events[0].context_message.lower()
    assert "shared code remains mounted read-only" in new.pending_context_events[0].context_message.lower()
    await manager.close()
    with pytest.raises(RuntimeError, match="closed"):
        await manager.get(key)


@pytest.mark.asyncio
async def test_retention_cleanup_tolerates_controller_deleting_sandbox_after_ownership_read() -> None:
    repository, backend = MemoryRepository(), FakeBackend()
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="expiry-delete-race")
    old = await manager.ensure(key)
    token = await repository.acquire_lease(key, "expiry-race-setup")
    stored = await repository.get(key)
    expired = stored.record.model_copy(update={
        "state": SessionState.SUSPENDED,
        "expires_at": datetime(2020, 1, 1, tzinfo=timezone.utc),
    })
    await repository.compare_and_swap(token, expired, expected_resource_version=stored.resource_version)
    await repository.release_lease(token)

    async def controller_won_delete_race(_ref):
        raise ResourceOwnershipError("owned Sandbox no longer exists")

    backend.delete = controller_won_delete_race
    new = await manager.ensure(key)

    assert old.generation == 1
    assert new.generation == 2
    assert new.pending_context_events[0].reason == "retention-expired"
    await manager.close()


@pytest.mark.asyncio
async def test_workspace_event_survives_failure_to_create_next_generation() -> None:
    from gke_session_sandbox import EventType, SessionEvent

    repository, backend = MemoryRepository(), FakeBackend()
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="lost-workspace")
    old = await manager.ensure(key)
    token = await repository.acquire_lease(key, "test-lost")
    stored = await repository.get(key)
    event = SessionEvent(
        event_id=f"{key.stable_hash()}:1:workspace:workspace_lost",
        generation=1,
        event_type=EventType.WORKSPACE_LOST,
        occurred_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        reason="external-pvc-loss",
        context_message="The previous workspace was lost; shared code remains mounted read-only.",
    )
    lost = stored.record.model_copy(update={
        "state": SessionState.LOST,
        "pending_events": (event,),
    })
    await repository.compare_and_swap(token, lost, expected_resource_version=stored.resource_version)
    await repository.release_lease(token)
    backend.fail_create = True

    with pytest.raises(RuntimeError, match="simulated create failure"):
        await manager.ensure(key)
    pending = (await manager.get(key)).pending_context_events
    assert pending == (event,)
    assert pending[0].event_id == event.event_id
    assert old.generation == 1
    await manager.close()


@pytest.mark.asyncio
async def test_terminate_and_stale_generation_handle() -> None:
    repository, backend = MemoryRepository(), FakeBackend()
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="thread")
    handle = await manager.ensure(key)
    terminated = await manager.terminate(handle, reason="test")
    assert terminated.state is SessionState.TERMINATED
    assert backend.deleted == [handle.sandbox_ref.name]
    assert len(terminated.pending_context_events) == 1
    assert terminated.pending_context_events[0].event_type.value == "workspace_cleared"
    assert terminated.pending_context_events[0].reason == "test"
    assert "shared code remains mounted read-only" in terminated.pending_context_events[0].context_message.lower()

    current = repository.records[key.stable_hash()]
    repository.records[key.stable_hash()] = StoredSession(
        current.record.model_copy(update={"generation": 2}),
        str(int(current.resource_version) + 1),
    )
    with pytest.raises(StaleGenerationError):
        await manager.resume(handle)
