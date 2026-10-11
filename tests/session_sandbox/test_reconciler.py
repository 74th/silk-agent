from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from gke_session_sandbox import (
    CommandRecord,
    CommandState,
    CoreSandboxBackend,
    DesiredSandboxState,
    ManagerConfig,
    ResourceRef,
    ResourceOwnershipError,
    SessionKey,
    SessionRecord,
    SessionState,
    SQLiteSessionRepository,
    SandboxManager,
)
from gke_session_sandbox.reconciler import LifecycleReconciler


def config() -> ManagerConfig:
    return ManagerConfig(
        runtime={"image": "example.invalid/sandbox:v1"},
        lifecycle={"reconcile_interval_seconds": 60, "idle_timeout_seconds": 300},
    )


def template() -> dict:
    return {
        "apiVersion": "agents.x-k8s.io/v1beta1",
        "kind": "Sandbox",
        "metadata": {"labels": {"app.kubernetes.io/name": "test"}},
        "spec": {"service": True, "podTemplate": {"spec": {"containers": [
            {"name": "sandboxd", "image": "example.invalid/sandbox:v1"}
        ]}}},
    }


class FakeBackend:
    def __init__(self) -> None:
        self.endpoint = ""
        self.suspended: list[str] = []

    async def create_with_policies(self, manifest, policies, *, namespace: str, name: str):
        ref = ResourceRef(api_version="agents.x-k8s.io/v1beta1", kind="Sandbox", namespace=namespace,
                          name=name, uid=f"uid-{name}")
        return ref, ()

    async def wait_ready(self, ref, **kwargs):
        return {"metadata": {"uid": ref.uid}, "status": {"podUID": f"pod-{ref.uid}"}}

    async def runtime_endpoint(self, ref, **kwargs):
        return self.endpoint

    async def suspend(self, ref):
        if getattr(self, "fail_suspend", False):
            raise RuntimeError("simulated suspend patch failure")
        self.suspended.append(ref.name)

    async def set_expiration(self, ref, expires_at):
        self.expiration = expires_at

    async def patch_annotations(self, ref, annotations):
        self.annotations = dict(annotations)

    async def wait_suspended(self, ref, **kwargs):
        return {"metadata": {"uid": ref.uid}}

    async def get_owned(self, ref):
        if getattr(self, "missing", False):
            from gke_session_sandbox.errors import ResourceOwnershipError
            raise ResourceOwnershipError("owned Sandbox no longer exists")
        metadata = {"uid": ref.uid}
        if getattr(self, "deleting", False):
            metadata["deletionTimestamp"] = "2030-01-01T00:00:00Z"
        return {"metadata": metadata}

    async def wait_deleted(self, ref, **kwargs):
        return None

    async def delete(self, ref):
        self.deleted = getattr(self, "deleted", []) + [ref.name]

    async def resume(self, ref):
        self.resumed = getattr(self, "resumed", []) + [ref.name]


async def setup_manager(tmp_path, *, gate_active_on_close: int = 0):
    gate_state = {"accepting": True}

    async def gate(request):
        body = await request.json()
        gate_state["accepting"] = body["accepting"]
        active = gate_active_on_close if not body["accepting"] else 0
        return web.json_response({"accepting": body["accepting"], "active_commands": active})

    async def command(_request):
        raise web.HTTPNotFound()

    app = web.Application()
    app.router.add_put("/v1/commands/gate", gate)
    app.router.add_get("/v1/commands/{command_id}", command)
    server = TestServer(app)
    await server.start_server()

    backend = FakeBackend()
    backend.endpoint = str(server.make_url("/" )).rstrip("/")
    repository = SQLiteSessionRepository(tmp_path / "sessions.sqlite3")
    manager = SandboxManager(config(), repository, backend, template=template())
    key = SessionKey(tenant_id="tenant", session_id="idle-test")
    handle = await manager.ensure(key)
    return manager, backend, repository, key, handle, gate_state, server


async def age_ready_session(repository, key, ready_at, **changes):
    token = await repository.acquire_lease(key, "test-setup")
    stored = await repository.get(key)
    record = stored.record.model_copy(update={"ready_at": ready_at, "idle_since": None, **changes})
    await repository.compare_and_swap(token, record, expected_resource_version=stored.resource_version)
    await repository.release_lease(token)


@pytest.mark.asyncio
async def test_idle_timeout_uses_last_command_or_ready_origin_and_suspends_at_300_seconds(tmp_path):
    manager, backend, repository, key, _handle, gate_state, server = await setup_manager(tmp_path)
    now = [datetime(2030, 1, 1, tzinfo=timezone.utc)]
    origin = now[0] - timedelta(seconds=299)
    await age_ready_session(repository, key, origin)
    reconciler = LifecycleReconciler(manager, clock=lambda: now[0])

    assert await reconciler.reconcile_once() == ()
    assert backend.suspended == []
    current = await repository.get(key)
    assert current.record.idle_since == origin
    assert current.record.last_no_commands_confirmed_at == now[0]

    now[0] += timedelta(seconds=1)
    assert await reconciler.reconcile_once() == ()
    assert backend.suspended == [current.record.sandbox_ref.name]
    current = await repository.get(key)
    assert current.record.state is SessionState.SUSPENDED
    assert current.record.suspended_at == now[0]
    assert not gate_state["accepting"]

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_active_file_operation_prevents_idle_suspend(tmp_path):
    manager, backend, repository, key, _handle, _gate_state, server = await setup_manager(tmp_path)
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    await age_ready_session(repository, key, now - timedelta(seconds=360), active_file_operations=1)

    assert await LifecycleReconciler(manager, clock=lambda: now).reconcile_once() == ()
    assert backend.suspended == []
    current = await repository.get(key)
    assert current.record.last_commands_checked_at == now
    assert current.record.active_file_operations == 1
    assert current.record.pending_events == ()

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_running_durable_command_prevents_idle_suspend_when_tracker_lacks_record(tmp_path):
    manager, backend, repository, key, _handle, _gate_state, server = await setup_manager(tmp_path)
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    command = CommandRecord(
        command_id="reserved-command", generation=1, runtime_instance_id="runtime",
        state=CommandState.RUNNING, accepted_at=now - timedelta(minutes=10),
    )
    await age_ready_session(
        repository, key, now - timedelta(minutes=10), active_commands=(command,),
    )

    assert await LifecycleReconciler(manager, clock=lambda: now).reconcile_once() == ()
    assert backend.suspended == []
    current = await repository.get(key)
    assert current.record.state is SessionState.READY
    assert current.record.active_commands[0].command_id == command.command_id
    assert current.record.idle_since is None

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_unknown_command_is_not_treated_as_idle(tmp_path):
    manager, backend, repository, key, _handle, _gate_state, server = await setup_manager(tmp_path)
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    unknown = CommandRecord(
        command_id="lost-command", generation=1, runtime_instance_id="runtime",
        state=CommandState.UNKNOWN, accepted_at=now - timedelta(minutes=10),
    )
    await age_ready_session(repository, key, now - timedelta(minutes=10), active_commands=(unknown,))

    assert await LifecycleReconciler(manager, clock=lambda: now).reconcile_once() == ()
    assert backend.suspended == []
    current = await repository.get(key)
    assert current.record.active_commands[0].state is CommandState.UNKNOWN
    assert current.record.idle_since is None

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_gate_closes_atomically_and_reopens_if_a_command_races_with_suspend(tmp_path):
    manager, backend, repository, key, _handle, gate_state, server = await setup_manager(
        tmp_path, gate_active_on_close=1
    )
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    await age_ready_session(repository, key, now - timedelta(seconds=360))

    assert await LifecycleReconciler(manager, clock=lambda: now).reconcile_once() == ()
    assert backend.suspended == []
    assert gate_state["accepting"] is True
    current = await repository.get(key)
    assert current.record.idle_since is None
    assert current.record.command_state_known is False

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_reconciler_recovers_suspending_state_after_process_restart(tmp_path):
    manager, backend, repository, key, handle, _gate_state, server = await setup_manager(tmp_path)
    token = await repository.acquire_lease(key, "test-setup")
    stored = await repository.get(key)
    pending = stored.record.model_copy(update={
        "state": SessionState.SUSPENDING,
        "desired_state": DesiredSandboxState.SUSPENDED,
        "operation_phase": "idle-suspend-intent",
    })
    await repository.compare_and_swap(token, pending, expected_resource_version=stored.resource_version)
    await repository.release_lease(token)

    assert await LifecycleReconciler(manager).reconcile_once() == ()
    recovered = await repository.get(key)
    assert recovered.record.state is SessionState.SUSPENDED
    assert backend.suspended == [handle.sandbox_ref.name]
    assert backend.expiration is not None

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_reconciler_recovers_terminating_state_idempotently(tmp_path):
    manager, backend, repository, key, handle, _gate_state, server = await setup_manager(tmp_path)
    token = await repository.acquire_lease(key, "test-setup")
    stored = await repository.get(key)
    pending = stored.record.model_copy(update={"state": SessionState.TERMINATING, "operation_phase": "terminate-intent"})
    await repository.compare_and_swap(token, pending, expected_resource_version=stored.resource_version)
    await repository.release_lease(token)

    assert await LifecycleReconciler(manager).reconcile_once() == ()
    recovered = await repository.get(key)
    assert recovered.record.state is SessionState.TERMINATED
    assert backend.deleted == [handle.sandbox_ref.name]

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_recover_process_state_clears_stale_file_counter_once(tmp_path):
    manager, _backend, repository, key, _handle, _gate_state, server = await setup_manager(tmp_path)
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    await age_ready_session(repository, key, now - timedelta(seconds=30), active_file_operations=2)

    await LifecycleReconciler(manager, clock=lambda: now).recover_process_state()
    recovered = await repository.get(key)
    assert recovered.record.active_file_operations == 0
    assert recovered.record.last_file_operation_at == now
    assert recovered.record.idle_since == now

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_reconciler_recovers_resuming_state_and_opens_command_gate(tmp_path):
    manager, backend, repository, key, handle, gate_state, server = await setup_manager(tmp_path)
    token = await repository.acquire_lease(key, "test-setup")
    stored = await repository.get(key)
    pending = stored.record.model_copy(update={
        "state": SessionState.RESUMING,
        "desired_state": DesiredSandboxState.RUNNING,
    })
    await repository.compare_and_swap(token, pending, expected_resource_version=stored.resource_version)
    await repository.release_lease(token)

    assert await LifecycleReconciler(manager).reconcile_once() == ()
    recovered = await repository.get(key)
    assert recovered.record.state is SessionState.READY
    assert recovered.record.expires_at is None
    assert backend.resumed == [handle.sandbox_ref.name]
    assert gate_state["accepting"] is True
    assert backend.annotations["session-sandbox.example.com/expires-at"] == ""

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_missing_sandbox_is_recorded_as_lost(tmp_path):
    manager, backend, repository, key, _handle, _gate_state, server = await setup_manager(tmp_path)
    backend.missing = True

    assert await LifecycleReconciler(manager).reconcile_once() == ()
    record = (await repository.get(key)).record
    assert record.state is SessionState.LOST
    assert record.command_state_known is False
    assert len(record.pending_events) == 1
    event = record.pending_events[0]
    assert event.event_type.value == "workspace_lost"
    assert event.generation == 1
    assert "previous workspace was lost" in event.context_message
    assert "shared code remains mounted read-only" in event.context_message
    assert event.event_id in {item.event_id for item in await manager.pending_context_events(key)}

    next_generation = await manager.ensure(key)
    assert next_generation.generation == 2
    assert next_generation.pending_context_events[0].event_id == event.event_id
    assert await manager.ack_context_event(key, event.event_id) is True
    assert await manager.ack_context_event(key, event.event_id) is False
    assert await manager.pending_context_events(key) == ()

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_missing_owned_workspace_pvc_marks_generation_lost(tmp_path):
    manager, _backend, repository, key, _handle, _gate_state, server = await setup_manager(tmp_path)
    pvc_ref = ResourceRef(
        api_version="v1", kind="PersistentVolumeClaim", namespace="session-sandboxes",
        name="workspace-1", uid="workspace-uid",
    )

    class MissingPVCManager:
        async def verify_owned(self, ref, *, sandbox_uid):
            raise ResourceOwnershipError("owned PVC no longer exists")

    manager._pvc_manager = MissingPVCManager()
    token = await repository.acquire_lease(key, "test-pvc")
    stored = await repository.get(key)
    updated = stored.record.model_copy(update={"workspace_pvc_refs": (pvc_ref,)})
    await repository.compare_and_swap(token, updated, expected_resource_version=stored.resource_version)
    await repository.release_lease(token)

    assert await LifecycleReconciler(manager).reconcile_once() == ()
    record = (await repository.get(key)).record
    assert record.state is SessionState.LOST
    assert record.pending_events[0].event_type.value == "workspace_lost"

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_lifespan_owns_and_stops_reconciler_task():
    started = asyncio.Event()
    stopped = asyncio.Event()
    reconciler = LifecycleReconciler(object())

    async def run_forever():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    reconciler.run_forever = run_forever
    async with reconciler.lifespan():
        await started.wait()
    assert stopped.is_set()


@pytest.mark.asyncio
async def test_expired_sandbox_already_deleting_is_not_deleted_twice(tmp_path):
    manager, backend, repository, key, handle, _gate_state, server = await setup_manager(tmp_path)
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    token = await repository.acquire_lease(key, "test-setup")
    stored = await repository.get(key)
    suspended = stored.record.model_copy(update={
        "state": SessionState.SUSPENDED,
        "expires_at": now - timedelta(seconds=1),
    })
    await repository.compare_and_swap(token, suspended, expected_resource_version=stored.resource_version)
    await repository.release_lease(token)
    backend.deleting = True

    assert await LifecycleReconciler(manager, clock=lambda: now).reconcile_once() == ()
    assert getattr(backend, "deleted", []) == []
    assert (await repository.get(key)).record.state is SessionState.TERMINATED

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_suspend_failure_keeps_gate_closed_and_recovers_from_durable_intent(tmp_path):
    manager, backend, repository, key, handle, gate_state, server = await setup_manager(tmp_path)
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    await age_ready_session(repository, key, now - timedelta(seconds=360))
    backend.fail_suspend = True
    reconciler = LifecycleReconciler(manager, clock=lambda: now)

    errors = await reconciler.reconcile_once()
    assert len(errors) == 1
    assert gate_state["accepting"] is False
    assert (await repository.get(key)).record.state is SessionState.SUSPENDING

    backend.fail_suspend = False
    assert await reconciler.reconcile_once() == ()
    assert (await repository.get(key)).record.state is SessionState.SUSPENDED
    assert backend.suspended == [handle.sandbox_ref.name]

    await manager.close()
    await server.close()
