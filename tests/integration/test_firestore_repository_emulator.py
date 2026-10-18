import asyncio
import os
import uuid
from datetime import UTC, datetime

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from gke_session_sandbox import (
    LifecycleReconciler,
    RepositoryConflictError,
    SandboxBusyError,
    SandboxManager,
    SessionKey,
    SessionLeaseHeldError,
    SessionLeaseLostError,
    SessionRecord,
    SessionState,
    SQLiteSessionRepository,
    StaleGenerationError,
)
from gke_session_sandbox.firestore_repository import (
    FirestoreSessionRepository,
    firestore_repository_contract,
)
from silk_agent.config import DistributedExecutionConfig
from silk_agent.stores.firestore.client import create_firestore_client
from tests.fakes.distributed_runtime import AsyncBarrier, FakeClock
from tests.session_sandbox.test_firestore_repository import complete_record
from tests.session_sandbox.test_manager import FakeBackend, config, template

pytestmark = pytest.mark.integration


@pytest.fixture
def repository_pair():
    host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run emulator tests")
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=f"silk-test-{uuid.uuid4().hex[:12]}"),
        emulator_host=host,
    )
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    clock = FakeClock(datetime(2026, 10, 14, tzinfo=UTC))
    first = FirestoreSessionRepository(client, collection_prefix=prefix, clock=clock.now)
    second = FirestoreSessionRepository(client, collection_prefix=prefix, clock=clock.now)
    try:
        yield first, second, clock
    finally:
        client.close()


async def test_sqlite_record_round_trips_through_firestore_with_cas(repository_pair, tmp_path):
    first, second, _ = repository_pair
    key = SessionKey(tenant_id="tenant", session_id="session")
    source = SQLiteSessionRepository(tmp_path / "source.sqlite3")
    try:
        source_token = await source.acquire_lease(key, "sqlite")
        original = await source.create(source_token, complete_record())
        token = await first.acquire_lease(key, "service-a")
        created = await first.create(token, original.record)
        restored = await second.get(key)
        assert restored == created
        assert restored.record == original.record
        assert (await second.list_records()) == (created,)

        changed = created.record.model_copy(update={"operation_phase": "changed"})
        saved = await first.compare_and_swap(
            token, changed, expected_resource_version=created.resource_version
        )
        assert saved.resource_version == "2"
        with pytest.raises(RepositoryConflictError):
            await second.compare_and_swap(
                token, changed, expected_resource_version=created.resource_version
            )
        with pytest.raises(StaleGenerationError):
            await first.compare_and_swap(
                token,
                changed.model_copy(update={"generation": 1}),
                expected_resource_version=saved.resource_version,
            )
    finally:
        await source.close()


async def test_reconciliation_candidates_are_filtered_and_paginated(repository_pair):
    first, _second, _clock = repository_pair
    base = complete_record()
    states = (SessionState.READY, SessionState.CREATING, SessionState.SUSPENDED)
    keys = [SessionKey(tenant_id="tenant", session_id=f"page-{index}") for index in range(3)]
    for key, state in zip(keys, states, strict=True):
        token = await first.acquire_lease(key, "service")
        await first.create(
            token,
            base.model_copy(update={"key_hash": key.stable_hash(), "state": state}),
        )

    expected = sorted(
        (
            (keys[0].stable_hash(), SessionState.READY),
            (keys[2].stable_hash(), SessionState.SUSPENDED),
        )
    )
    page, cursor = await first.list_reconciliation_candidates(limit=1)
    assert [(item.record.key_hash, item.record.state) for item in page] == [expected[0]]
    assert cursor == expected[0][0]
    page, cursor = await first.list_reconciliation_candidates(limit=1, cursor=cursor)
    assert [(item.record.key_hash, item.record.state) for item in page] == [expected[1]]
    assert cursor is None


async def test_expired_and_reacquired_leases_fence_every_mutation(repository_pair):
    first, second, clock = repository_pair
    key = SessionKey(tenant_id="tenant", session_id="session")
    old = await first.acquire_lease(key, "service-a")
    stored = await first.create(old, complete_record())
    with pytest.raises(SessionLeaseHeldError):
        await second.acquire_lease(key, "service-b")
    assert await first.renew_lease(old) == old
    clock.advance(60)
    new = await second.acquire_lease(key, "service-a")
    assert new.epoch == old.epoch + 1
    actions = (
        lambda: first.create(old, stored.record),
        lambda: first.compare_and_swap(old, stored.record, expected_resource_version="1"),
        lambda: first.reserve_command(
            old, generation=2, command_id="command-2", runtime_instance_id="runtime-2"
        ),
        lambda: first.acknowledge_event(old, "notice-2"),
        lambda: first.append_event_once(old, stored.record.pending_events[0]),
        lambda: first.record_operation(old, operation_id="new-op", operation_phase="accepted"),
        lambda: first.adjust_file_operations(old, 1),
        lambda: first.renew_lease(old),
        lambda: first.release_lease(old),
    )
    for action in actions:
        with pytest.raises(SessionLeaseLostError):
            await action()
    assert await second.get(key) == stored
    assert await second.acknowledge_event(new, "notice-2")
    assert not await second.acknowledge_event(new, "notice-2")
    with pytest.raises(StaleGenerationError):
        await second.reserve_command(
            new, generation=1, command_id="other", runtime_instance_id="runtime-2"
        )
    _, reserved = await second.reserve_command(
        new, generation=2, command_id="new-command", runtime_instance_id="runtime-2"
    )
    assert reserved
    _, reserved = await second.reserve_command(
        new, generation=2, command_id="new-command", runtime_instance_id="runtime-2"
    )
    assert not reserved


async def test_release_and_reacquire_by_same_holder_never_revives_old_token(repository_pair):
    first, second, _ = repository_pair
    key = SessionKey(tenant_id="tenant", session_id="session")
    old = await first.acquire_lease(key, "service-a")
    await first.release_lease(old)
    new = await second.acquire_lease(key, "service-a")
    assert new.epoch > old.epoch
    with pytest.raises(SessionLeaseLostError):
        await first.create(old, complete_record())
    await second.create(new, complete_record())


async def test_two_managers_share_one_sandbox_when_ensure_competes(repository_pair):
    first, second, _ = repository_pair
    barrier = AsyncBarrier()

    class BlockingBackend(FakeBackend):
        async def wait_ready(self, ref, **kwargs):
            await barrier.wait()
            return await super().wait_ready(ref, **kwargs)

    backend = BlockingBackend()
    managers = (
        SandboxManager(
            config().model_copy(update={"lease_acquire_timeout_seconds": 1}),
            firestore_repository_contract(first),
            backend,
            template=template(),
            holder_identity="service-a",
        ),
        SandboxManager(
            config().model_copy(update={"lease_acquire_timeout_seconds": 1}),
            firestore_repository_contract(second),
            backend,
            template=template(),
            holder_identity="service-b",
        ),
    )
    key = SessionKey(tenant_id="tenant", session_id="competing")
    preparing = asyncio.create_task(managers[0].ensure(key))
    await barrier.wait_until_entered()
    try:
        with pytest.raises(SessionLeaseHeldError):
            await managers[1].ensure(key)
    finally:
        barrier.release()
        await preparing
    assert len(backend.created) == 1

    await managers[1].ensure(key)
    assert len(backend.created) == 1


async def test_two_managers_suspend_the_sandbox_only_once(repository_pair):
    first, second, _ = repository_pair
    barrier = AsyncBarrier()

    class BlockingBackend(FakeBackend):
        async def suspend(self, ref):
            await barrier.wait()
            await super().suspend(ref)

    async def gate(_request):
        return web.json_response({"active_commands": 0})

    app = web.Application()
    app.router.add_put("/v1/commands/gate", gate)
    async with TestServer(app) as server:
        backend = BlockingBackend()
        backend.endpoint = str(server.make_url("/")).rstrip("/")
        managers = (
            SandboxManager(
                config().model_copy(update={"lease_acquire_timeout_seconds": 1}),
                first,
                backend,
                template=template(),
                holder_identity="a",
            ),
            SandboxManager(
                config().model_copy(update={"lease_acquire_timeout_seconds": 1}),
                second,
                backend,
                template=template(),
                holder_identity="b",
            ),
        )
        key = SessionKey(tenant_id="tenant", session_id="suspending")
        handle = await managers[0].ensure(key)
        suspending = asyncio.create_task(managers[0].suspend(handle))
        await barrier.wait_until_entered()
        try:
            with pytest.raises(SessionLeaseHeldError):
                await managers[1].suspend(handle)
        finally:
            barrier.release()
            suspended = await suspending
        assert suspended.state is SessionState.SUSPENDED
        assert (await managers[1].suspend(handle)).state is SessionState.SUSPENDED
        assert len(backend.suspended) == 1


async def test_repository_refuses_oversized_records_without_committing(repository_pair):
    first, _, _ = repository_pair
    key = SessionKey(tenant_id="tenant", session_id="large")
    token = await first.acquire_lease(key, "service-a")
    from gke_session_sandbox import RepositoryCapacityError

    with pytest.raises(RepositoryCapacityError):
        await first.create(
            token,
            SessionRecord(
                key_hash=key.stable_hash(),
                generation=1,
                state=SessionState.CREATING,
                last_error="x" * 1024 * 1024,
            ),
        )
    assert await first.get(key) is None


async def test_manager_restart_preserves_another_executors_file_operation(repository_pair):
    first, second, clock = repository_pair

    async def gate(_request):
        return web.json_response({"active_commands": 0})

    app = web.Application()
    app.router.add_put("/v1/commands/gate", gate)
    async with TestServer(app) as server:
        backend = FakeBackend()
        backend.endpoint = str(server.make_url("/")).rstrip("/")
        manager_a = SandboxManager(
            config(), first, backend, template=template(), holder_identity="service-a"
        )
        manager_b = SandboxManager(
            config(), second, backend, template=template(), holder_identity="service-b"
        )
        key = SessionKey(tenant_id="tenant", session_id="active-file")
        handle = await manager_a.ensure(key)
        async with manager_a._file_operation(key, handle.generation):
            assert (await second.get(key)).record.active_file_operations == 1
            clock.advance(120)
            await LifecycleReconciler(manager_b).recover_process_state()
            assert (await second.get(key)).record.active_file_operations == 1
            with pytest.raises(SandboxBusyError):
                await manager_b.suspend(handle)
            assert not backend.suspended
        completed = await second.get(key)
        assert completed.record.active_file_operations == 0
        assert completed.record.idle_since is not None
        await manager_b.suspend(handle)
        assert len(backend.suspended) == 1


async def test_cancelled_file_request_retains_unknown_activity_after_restart(repository_pair):
    first, second, clock = repository_pair
    manager = SandboxManager(
        config(), first, FakeBackend(), template=template(), holder_identity="service-a"
    )
    key = SessionKey(tenant_id="tenant", session_id="uncertain-file")
    handle = await manager.ensure(key)
    with pytest.raises(asyncio.CancelledError):
        async with manager._file_operation(key, handle.generation):
            raise asyncio.CancelledError
    restarted = SandboxManager(config(), second, manager.backend, template=template())
    clock.advance(120)
    await LifecycleReconciler(restarted).recover_process_state()
    record = await second.get(key)
    assert record.record.active_file_operations == 1
    assert record.record.idle_since is None
    record_ref, _ = first._refs(key.stable_hash())
    operations = [
        item.to_dict() async for item in record_ref.collection("file_operations").stream()
    ]
    assert len(operations) == 1
    assert operations[0]["state"] == "unknown"
    assert operations[0]["executor_id"] == "service-a"
