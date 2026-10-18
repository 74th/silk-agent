from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from gke_session_sandbox import (
    CommandRecord,
    CommandState,
    ConfigurationError,
    ManagerConfig,
    ResourceOwnershipError,
    ResourceRef,
    SandboxManager,
    SessionConfigurationConflictError,
    SessionKey,
    SessionLeaseHeldError,
    SessionRecord,
    SessionState,
    StaleGenerationError,
    StoredSession,
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
        self.file_operations = {}

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

    async def get_by_hash(self, key_hash: str):
        return self.records.get(key_hash)

    async def create(self, token, record: SessionRecord) -> StoredSession:
        if record.key_hash in self.records:
            raise RuntimeError("duplicate record")
        self.version += 1
        stored = StoredSession(record, str(self.version))
        self.records[record.key_hash] = stored
        return stored

    async def compare_and_swap(
        self, token, record: SessionRecord, *, expected_resource_version: str
    ) -> StoredSession:
        current = self.records[record.key_hash]
        if current.resource_version != expected_resource_version:
            raise RuntimeError("CAS conflict")
        self.version += 1
        stored = StoredSession(record, str(self.version))
        self.records[record.key_hash] = stored
        return stored

    async def list_records(self):
        return tuple(self.records.values())

    async def reserve_command(
        self, token, *, generation: int, command_id: str, runtime_instance_id: str
    ):
        stored = self.records[token.key_hash]
        record = stored.record
        for command in record.active_commands:
            if command.command_id == command_id:
                return command, False
        command = CommandRecord(
            command_id=command_id,
            generation=generation,
            runtime_instance_id=runtime_instance_id,
            state=CommandState.RUNNING,
            accepted_at=datetime.now(UTC),
        )
        updated = record.model_copy(update={"active_commands": (*record.active_commands, command)})
        await self.compare_and_swap(
            token, updated, expected_resource_version=stored.resource_version
        )
        return command, True

    async def adjust_file_operations(self, token, delta: int):
        stored = self.records[token.key_hash]
        record = stored.record.model_copy(
            update={
                "active_file_operations": stored.record.active_file_operations + delta,
            }
        )
        return await self.compare_and_swap(
            token, record, expected_resource_version=stored.resource_version
        )

    async def begin_file_operation(
        self,
        token,
        *,
        operation_id,
        generation,
        executor_id,
        expires_at,
    ):
        return await self._change_file_operation(
            token, operation_id, generation, executor_id, expires_at
        )

    async def finish_file_operation(
        self, token, *, operation_id, generation, executor_id, confirmed=True
    ):
        return await self._change_file_operation(
            token, operation_id, generation, executor_id, None, confirmed=confirmed
        )

    async def _change_file_operation(
        self, token, operation_id, generation, executor_id, expires_at, confirmed=True
    ):
        from gke_session_sandbox.file_operations import change_file_operation

        stored = self.records[token.key_hash]
        key = (token.key_hash, operation_id)
        record, operation, changed = change_file_operation(
            stored.record,
            self.file_operations.get(key),
            operation_id=operation_id,
            generation=generation,
            executor_id=executor_id,
            expires_at=expires_at,
            now=datetime.now(UTC),
            confirmed=confirmed,
        )
        if not changed:
            return stored
        saved = await self.compare_and_swap(
            token, record, expected_resource_version=stored.resource_version
        )
        self.file_operations[key] = operation
        return saved


class FakeBackend:
    def __init__(self) -> None:
        self.created: list[tuple[str, dict, tuple[dict, ...]]] = []
        self.deleted: list[str] = []
        self.suspended: list[str] = []
        self.started: list[str] = []
        self.ready_timeouts: list[object] = []
        self.api_client = None
        self.endpoint = ""
        self.endpoint_refs: list[ResourceRef] = []
        self.diagnostics: list[tuple[str, list[str], str]] = []
        self.applied: list[tuple[str, tuple[dict, ...], ResourceRef]] = []
        self.deleted_policies: list[tuple[tuple[str, str, str], ...]] = []

    async def exec_diagnostic(self, ref, command, *, container, timeout_seconds=60):
        self.diagnostics.append((ref.name, list(command), container))
        return 0, "ok\n"

    async def apply_policies(self, namespace, policies, *, owner):
        self.applied.append((namespace, tuple(deepcopy(dict(x)) for x in policies), owner))
        return ()

    async def delete_policies(self, namespace, names, *, owner, labels):
        self.deleted_policies.append(tuple(names))
        return ()

    async def create_with_policies(self, manifest, policies, *, namespace: str, name: str):
        if getattr(self, "fail_create", False):
            raise RuntimeError("simulated create failure")
        self.created.append(
            (name, deepcopy(dict(manifest)), tuple(deepcopy(dict(x)) for x in policies))
        )
        ref = ResourceRef(
            api_version="agents.x-k8s.io/v1beta1",
            kind="Sandbox",
            namespace=namespace,
            name=name,
            uid=f"uid-{name}",
        )
        return ref, ()

    async def wait_ready(self, ref, **kwargs):
        self.ready_timeouts.append(kwargs.get("timeout_seconds"))
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


def config(**runtime: object) -> ManagerConfig:
    return ManagerConfig(
        runtime={"image": "example.invalid/sandbox:v1", **runtime},
        environment_fingerprint_key="a-stable-secret-key-with-at-least-32-bytes",
    )


def template() -> dict:
    return {
        "apiVersion": "agents.x-k8s.io/v1beta1",
        "kind": "Sandbox",
        "metadata": {"labels": {"app.kubernetes.io/name": "test"}},
        "spec": {
            "service": True,
            "podTemplate": {
                "spec": {
                    "containers": [
                        {
                            "name": "sandboxd",
                            "image": "example.invalid/sandbox:v1",
                            "env": [{"name": "USER_EMAIL", "value": "default@example.com"}],
                        }
                    ]
                }
            },
        },
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
        "SESSION_SANDBOX_COMMAND_OUTPUT_LIMIT_BYTES": str(
            config().lifecycle.command_output_limit_bytes
        ),
        "SESSION_SANDBOX_SESSION_OUTPUT_LIMIT_BYTES": str(
            config().lifecycle.session_output_limit_bytes
        ),
        "SESSION_SANDBOX_BUFFERED_RESPONSE_LIMIT_BYTES": str(
            config().lifecycle.buffered_response_limit_bytes
        ),
        "SESSION_SANDBOX_OUTPUT_RETENTION_SECONDS": str(config().lifecycle.retention_seconds),
    }
    with pytest.raises(SessionConfigurationConflictError):
        await manager.ensure(first_key, env={"USER_EMAIL": "changed@example.com"})


@pytest.mark.asyncio
async def test_simultaneous_ensure_for_same_key_uses_one_sandbox() -> None:
    repository, backend = MemoryRepository(), FakeBackend()
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="concurrent")

    first, second = await pytest.importorskip("asyncio").gather(
        manager.ensure(key), manager.ensure(key)
    )

    assert first.sandbox_ref == second.sandbox_ref
    assert len(backend.created) == 1


@pytest.mark.asyncio
async def test_session_command_client_ensures_session_and_resolves_current_endpoint() -> None:
    received = []

    async def create(request):
        received.append(await request.json())
        return web.json_response(
            {"command_id": received[-1]["command_id"], "state": "running"}, status=202
        )

    async def command_state(_request):
        return web.json_response(
            {
                "command_id": "client-command",
                "state": "finished",
                "exit_code": 0,
                "started_at": "2030-01-01T00:00:00Z",
                "finished_at": "2030-01-01T00:00:01Z",
            }
        )

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
async def test_resume_applies_current_policies_and_removes_unneeded_optional_ones() -> None:
    async def gate(request):
        body = await request.json()
        return web.json_response({"accepting": body["accepting"], "active_commands": 0})

    app = web.Application()
    app.router.add_put("/v1/commands/gate", gate)
    server = TestServer(app)
    await server.start_server()
    repository, backend = MemoryRepository(), FakeBackend()
    backend.endpoint = str(server.make_url("/")).rstrip("/")
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="thread")
    handle = await manager.ensure(key)
    suspended = await manager.suspend(handle)

    await manager.resume(suspended)
    await manager.close()
    await server.close()

    namespace, policies, owner = backend.applied[-1]
    assert namespace == "session-sandboxes"
    assert owner == handle.sandbox_ref
    dns = next(p for p in policies if "dns-egress" in p["metadata"]["name"])
    assert [t["podSelector"]["matchLabels"] for t in dns["spec"]["egress"][0]["to"]] == [
        {"k8s-app": "kube-dns"},
        {"k8s-app": "node-local-dns"},
    ]
    # No FQDN / metadata allowance is configured, so any left over from older settings go.
    assert [name[1] for name in backend.deleted_policies[-1]] == [
        "FQDNNetworkPolicy",
        "NetworkPolicy",
    ]
    assert all("fqdn" not in p["metadata"]["name"] for p in policies)


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
    backend.endpoint = str(server.make_url("/")).rstrip("/")
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
async def test_ensure_after_retention_deadline_cleans_old_generation_before_creating_new_one() -> (
    None
):
    repository, backend = MemoryRepository(), FakeBackend()
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="expired")
    old = await manager.ensure(key)
    token = await repository.acquire_lease(key, "expire-setup")
    stored = await repository.get(key)
    expired = stored.record.model_copy(
        update={
            "state": SessionState.SUSPENDED,
            "expires_at": datetime(2020, 1, 1, tzinfo=UTC),
        }
    )
    await repository.compare_and_swap(
        token, expired, expected_resource_version=stored.resource_version
    )
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
    assert (
        "shared code remains mounted read-only"
        in new.pending_context_events[0].context_message.lower()
    )
    await manager.close()
    with pytest.raises(RuntimeError, match="closed"):
        await manager.get(key)


@pytest.mark.asyncio
async def test_retention_cleanup_tolerates_controller_deleting_sandbox_after_ownership_read() -> (
    None
):
    repository, backend = MemoryRepository(), FakeBackend()
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="expiry-delete-race")
    old = await manager.ensure(key)
    token = await repository.acquire_lease(key, "expiry-race-setup")
    stored = await repository.get(key)
    expired = stored.record.model_copy(
        update={
            "state": SessionState.SUSPENDED,
            "expires_at": datetime(2020, 1, 1, tzinfo=UTC),
        }
    )
    await repository.compare_and_swap(
        token, expired, expected_resource_version=stored.resource_version
    )
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
        occurred_at=datetime(2030, 1, 1, tzinfo=UTC),
        reason="external-pvc-loss",
        context_message="The previous workspace was lost; shared code remains mounted read-only.",
    )
    lost = stored.record.model_copy(
        update={
            "state": SessionState.LOST,
            "pending_events": (event,),
        }
    )
    await repository.compare_and_swap(
        token, lost, expected_resource_version=stored.resource_version
    )
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
    assert (
        "shared code remains mounted read-only"
        in terminated.pending_context_events[0].context_message.lower()
    )

    current = repository.records[key.stable_hash()]
    repository.records[key.stable_hash()] = StoredSession(
        current.record.model_copy(update={"generation": 2}),
        str(int(current.resource_version) + 1),
    )
    with pytest.raises(StaleGenerationError):
        await manager.resume(handle)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runtime", "expected"), [({}, 180.0), ({"ready_timeout_seconds": 600}, 600.0)]
)
async def test_create_and_resume_wait_for_the_configured_ready_timeout(runtime, expected) -> None:
    async def gate(request):
        body = await request.json()
        return web.json_response({"accepting": body["accepting"], "active_commands": 0})

    app = web.Application()
    app.router.add_put("/v1/commands/gate", gate)
    server = TestServer(app)
    await server.start_server()
    repository, backend = MemoryRepository(), FakeBackend()
    backend.endpoint = str(server.make_url("/")).rstrip("/")
    manager = SandboxManager(config(**runtime), repository, backend, template=template())
    handle = await manager.ensure(SessionKey(tenant_id="tenant", session_id="slow-start"))
    await manager.resume(await manager.suspend(handle))
    await manager.close()
    await server.close()

    assert backend.ready_timeouts == [expected, expected]


async def _ready_manager(backend: FakeBackend | None = None):
    repository, backend = MemoryRepository(), backend or FakeBackend()
    manager = SandboxManager(config(), repository, backend, template=template())
    return manager, repository, backend


@pytest.mark.asyncio
async def test_reapply_network_policies_applies_under_lease_without_touching_the_sandbox() -> None:
    manager, repository, backend = await _ready_manager()
    key = SessionKey(tenant_id="tenant", session_id="thread")
    handle = await manager.ensure(key)

    results = await manager.reapply_network_policies(key)

    assert [(r.key_hash, r.status) for r in results] == [(key.stable_hash(), "applied")]
    assert backend.applied[-1][2] == handle.sandbox_ref
    assert backend.started == [] and backend.suspended == [] and backend.deleted == []
    assert repository.holders == {}


@pytest.mark.asyncio
async def test_reapply_network_policies_skips_sessions_whose_lease_is_held() -> None:
    manager, repository, backend = await _ready_manager()
    held = SessionKey(tenant_id="tenant", session_id="held")
    free = SessionKey(tenant_id="tenant", session_id="free")
    await manager.ensure(held)
    await manager.ensure(free)
    repository.holders[held.stable_hash()] = "lifecycle-operation"
    backend.applied.clear()

    async def get_owned(ref):
        key = held if ref.name.startswith(f"session-{held.stable_hash()[:10]}") else free
        return {
            "metadata": {
                "uid": ref.uid,
                "annotations": {
                    "session-sandbox.example.com/tenant-id": key.tenant_id,
                    "session-sandbox.example.com/session-id": key.session_id,
                },
            }
        }

    backend.get_owned = get_owned
    results = {r.key_hash: r for r in await manager.reapply_network_policies()}

    assert results[held.stable_hash()].status == "skipped"
    assert "lease" in results[held.stable_hash()].detail
    assert results[free.stable_hash()].status == "applied"
    assert len(backend.applied) == 1
    assert repository.holders == {held.stable_hash(): "lifecycle-operation"}


@pytest.mark.asyncio
async def test_reapply_network_policies_reports_a_foreign_policy_as_failed() -> None:
    manager, _repository, backend = await _ready_manager()
    key = SessionKey(tenant_id="tenant", session_id="thread")
    await manager.ensure(key)

    async def refuse(namespace, policies, *, owner):
        raise ResourceOwnershipError("NetworkPolicy name already belongs to another owner")

    backend.apply_policies = refuse
    (result,) = await manager.reapply_network_policies(key)

    assert result.status == "failed"
    assert "another owner" in result.detail


async def _set_state(repository, key, **changes):
    token = await repository.acquire_lease(key, "test-setup")
    stored = await repository.get(key)
    await repository.compare_and_swap(
        token,
        stored.record.model_copy(update=changes),
        expected_resource_version=stored.resource_version,
    )
    await repository.release_lease(token)


@pytest.mark.asyncio
async def test_ensure_from_resuming_applies_policies_first_and_stays_resuming_on_failure() -> None:
    async def gate(request):
        body = await request.json()
        return web.json_response({"accepting": body["accepting"], "active_commands": 0})

    app = web.Application()
    app.router.add_put("/v1/commands/gate", gate)
    async with TestServer(app) as server:
        manager, repository, backend = await _ready_manager()
        backend.endpoint = str(server.make_url("/")).rstrip("/")
        key = SessionKey(tenant_id="tenant", session_id="thread")
        handle = await manager.ensure(key)
        await _set_state(repository, key, state=SessionState.RESUMING)

        healthy_apply = backend.apply_policies

        async def refuse(namespace, policies, *, owner):
            raise ResourceOwnershipError("NetworkPolicy name already belongs to another owner")

        backend.apply_policies = refuse
        backend.started.clear()
        with pytest.raises(ResourceOwnershipError, match="another owner"):
            await manager.ensure(key)
        assert repository.records[key.stable_hash()].record.state is SessionState.RESUMING
        assert backend.started == []

        backend.apply_policies = healthy_apply
        recovered = await manager.ensure(key)
        assert recovered.state is SessionState.READY
        assert backend.started == [handle.sandbox_ref.name]
        assert backend.applied[-1][2] == handle.sandbox_ref
        await manager.close()


OWNER_ENV = {"AGENT_USER_EMAIL": "owner@example.com"}


async def _suspend_expired(repository, key):
    token = await repository.acquire_lease(key, "expire-setup")
    stored = await repository.get(key)
    expired = stored.record.model_copy(
        update={"state": SessionState.SUSPENDED, "expires_at": datetime(2020, 1, 1, tzinfo=UTC)}
    )
    await repository.compare_and_swap(
        token, expired, expected_resource_version=stored.resource_version
    )
    await repository.release_lease(token)


@pytest.mark.asyncio
async def test_new_generation_env_does_not_conflict_with_a_generation_created_without_env() -> None:
    # A Sandbox from before the owner's e-mail was passed: created with no env at all.
    async def gate(request):
        body = await request.json()
        return web.json_response({"accepting": body["accepting"], "active_commands": 0})

    app = web.Application()
    app.router.add_put("/v1/commands/gate", gate)
    server = TestServer(app)
    await server.start_server()
    backend = FakeBackend()
    backend.endpoint = str(server.make_url("/")).rstrip("/")
    manager, repository, backend = await _ready_manager(backend)
    key = SessionKey(tenant_id="tenant", session_id="legacy")
    legacy = await manager.ensure(key)
    assert legacy.record.creation_env_fingerprint is None

    same = await manager.ensure(key, new_generation_env=OWNER_ENV)  # READY: no conflict
    assert same.generation == legacy.generation
    assert same.record.creation_env_fingerprint is None  # it keeps running as it was

    await manager.suspend(key)
    resumed = await manager.ensure(key, new_generation_env=OWNER_ENV)  # SUSPENDED: resumes
    assert resumed.generation == legacy.generation

    # The strict `env` still refuses it, as before.
    with pytest.raises(SessionConfigurationConflictError):
        await manager.ensure(key, env=OWNER_ENV)
    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_new_generation_env_applies_to_a_new_session_and_to_the_next_generation() -> None:
    manager, repository, backend = await _ready_manager()
    fresh = SessionKey(tenant_id="tenant", session_id="fresh")
    created = await manager.ensure(fresh, new_generation_env=OWNER_ENV)
    assert created.record.creation_env_fingerprint is not None
    again = await manager.ensure(fresh, new_generation_env=OWNER_ENV)
    assert again.generation == created.generation  # idempotent

    legacy = SessionKey(tenant_id="tenant", session_id="legacy-expired")
    old = await manager.ensure(legacy)
    await _suspend_expired(repository, legacy)
    new = await manager.ensure(legacy, new_generation_env=OWNER_ENV)
    assert (old.generation, new.generation) == (1, 2)
    assert new.record.creation_env_fingerprint is not None  # the e-mail starts here


@pytest.mark.asyncio
async def test_env_and_new_generation_env_are_mutually_exclusive() -> None:
    manager, _repository, _backend = await _ready_manager()
    with pytest.raises(ValueError):
        await manager.ensure(
            SessionKey(tenant_id="tenant", session_id="both"),
            env=OWNER_ENV,
            new_generation_env=OWNER_ENV,
        )


@pytest.mark.asyncio
async def test_observe_commands_never_resumes_or_creates_a_sandbox() -> None:
    from gke_session_sandbox.errors import SandboxLostError, SandboxNotReadyError

    manager, repository, backend = await _ready_manager()
    key = SessionKey(tenant_id="tenant", session_id="observed")
    handle = await manager.ensure(key)
    started_before = list(backend.started)
    generation_before = handle.generation

    await _set_state(repository, key, state=SessionState.SUSPENDED)
    with pytest.raises(SandboxNotReadyError):
        await handle.observe_commands.get("any-command")

    await _set_state(repository, key, state=SessionState.LOST)
    with pytest.raises(SandboxLostError):
        await handle.observe_commands.get("any-command")

    await _set_state(repository, key, state=SessionState.TERMINATED)
    with pytest.raises(SandboxLostError):
        await handle.observe_commands.get("any-command")

    # Nothing was resumed, created or started, and the stored generation did not move.
    assert backend.started == started_before and backend.endpoint_refs == []
    assert repository.records[key.stable_hash()].record.generation == generation_before


@pytest.mark.asyncio
async def test_observe_commands_reads_status_and_output_of_a_ready_sandbox() -> None:
    async def command_state(_request):
        return web.json_response(
            {"command_id": "c", "state": "running", "started_at": "2030-01-01T00:00:00Z"}
        )

    async def command_output(_request):
        return web.json_response(
            {
                "command_id": "c",
                "stream": "stdout",
                "data": "aGVsbG8=",
                "cursor": 5,
                "total_bytes": 5,
                "complete": True,
                "output_complete": True,
            }
        )

    app = web.Application()
    app.router.add_get("/v1/commands/{command_id}", command_state)
    app.router.add_get("/v1/commands/{command_id}/output", command_output)
    async with TestServer(app) as server:
        repository, backend = MemoryRepository(), FakeBackend()
        backend.endpoint = str(server.make_url("")).rstrip("/")
        manager = SandboxManager(config(), repository, backend, template=template())
        handle = await manager.ensure(SessionKey(tenant_id="tenant", session_id="observed-ok"))
        async with handle.observe_commands as commands:
            record = await commands.get("c")
            page = await commands.read_output("c", stream="stdout", cursor=0)
        assert record["state"] == "running"
        assert (page.data, page.cursor, page.complete) == (b"hello", 5, True)
        assert backend.endpoint_refs == [handle.sandbox_ref]  # resolved without ensure()
        await manager.close()


@pytest.mark.asyncio
async def test_observe_commands_of_an_older_generation_is_stale() -> None:
    manager, repository, _backend = await _ready_manager()
    key = SessionKey(tenant_id="tenant", session_id="replaced")
    handle = await manager.ensure(key)
    await _set_state(repository, key, generation=handle.generation + 1)
    with pytest.raises(StaleGenerationError):
        await handle.observe_commands.get("any-command")


@pytest.mark.asyncio
async def test_diagnostic_command_refuses_a_suspended_session_and_changes_nothing() -> None:
    manager, repository, backend = await _ready_manager()
    key = SessionKey(tenant_id="tenant", session_id="thread")
    handle = await manager.ensure(key)
    await _set_state(repository, key, state=SessionState.SUSPENDED)
    before = repository.records[key.stable_hash()]
    backend.started.clear()

    with pytest.raises(ConfigurationError, match="suspended.*do not resume"):
        await manager.run_diagnostic_command(key, ["true"])

    assert repository.records[key.stable_hash()] == before
    assert backend.started == [] and backend.endpoint_refs == []
    assert handle.state is SessionState.READY


@pytest.mark.asyncio
async def test_diagnostic_command_bypasses_the_tracker_and_leaves_stored_state_unchanged() -> None:
    manager, repository, backend = await _ready_manager()
    # No tracker endpoint exists in this test: any request to it would fail.
    key = SessionKey(tenant_id="tenant", session_id="thread")
    handle = await manager.ensure(key)
    before = repository.records[key.stable_hash()]

    result = await manager.run_diagnostic_command(key, ["true"])

    assert (result.exit_code, result.output) == (0, "ok\n")
    assert backend.diagnostics == [(handle.sandbox_ref.name, ["true"], "sandboxd")]
    assert backend.endpoint_refs == []
    assert repository.records[key.stable_hash()] == before
