"""Worker role composition against the Firestore Emulator with a fake SDK and Sandbox."""

import asyncio
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from silk_agent.config import Config, DistributedExecutionConfig
from silk_agent.core.clock import SystemClock
from silk_agent.core.distributed_models import WorkerRecord
from silk_agent.distributed import worker_role
from silk_agent.distributed.control_runtime import ControlAllocatorLoop
from silk_agent.distributed.worker import DistributedWorker
from silk_agent.distributed.worker_role import WorkerActorFactory
from silk_agent.guards.noop import AllowAllGuard, NoopSanitizer
from silk_agent.stores.firestore.allocator import FirestoreAllocator
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.command_monitor import FirestoreCommandMonitorStore
from silk_agent.stores.firestore.conversations import FirestoreConversationStore
from silk_agent.stores.firestore.inbox import FirestoreInboxStore
from silk_agent.stores.firestore.outbox import FirestoreOutboxWriter
from silk_agent.stores.firestore.ownership import FirestoreOwnershipStore
from silk_agent.stores.firestore.rate_limits import FirestoreUserRateLimitStore
from silk_agent.stores.firestore.workers import FirestoreWorkerDirectory
from silk_agent.testing.fakes import FakeAgentEngine, InMemoryRuntime, Text

pytestmark = pytest.mark.integration

SENDER = {"email": "user@example.com", "is_guest": False, "is_external": False, "team": "T"}


async def _until(predicate, *, seconds=15.0):
    deadline = asyncio.get_running_loop().time() + seconds
    while asyncio.get_running_loop().time() < deadline:
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(0.1)
    raise AssertionError("condition was not reached")


@pytest.mark.asyncio
async def test_worker_role_runs_input_stop_and_reset_through_firestore(monkeypatch, tmp_path: Path):
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    monkeypatch.setattr(worker_role, "build_input_guard", lambda _config: AllowAllGuard())
    monkeypatch.setattr(worker_role, "build_output_sanitizer", lambda _config: NoopSanitizer())

    prefix = f"test-{uuid.uuid4().hex[:12]}"
    dist = DistributedExecutionConfig(firestore_project=f"silk-agent-test-{uuid.uuid4().hex[:12]}")
    client = create_firestore_client(dist, emulator_host=emulator_host)
    clock = SystemClock()
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    ownership = FirestoreOwnershipStore(client, collection_prefix=prefix)
    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    key = f"slack:T:C:{uuid.uuid4().hex}"
    runtime = InMemoryRuntime()

    def engine_builder(_config, _runtime, _clock, **_kwargs):
        return lambda _key: FakeAgentEngine(clock, [Text("reply")])

    config = Config()
    factory = WorkerActorFactory(
        config=config,
        clock=clock,
        inbox=inbox,
        conversations=conversations,
        outbox_writer=FirestoreOutboxWriter(client, collection_prefix=prefix),
        rate_limits=FirestoreUserRateLimitStore(client, collection_prefix=prefix),
        command_monitor_store=FirestoreCommandMonitorStore(client, collection_prefix=prefix),
        runtime=runtime,
        session_store=None,
        skills=None,
        work_root=tmp_path,
        monitor_id="worker-a",
        build_engine_factory_fn=engine_builder,
        poll_seconds=0.05,
    )

    async def drain(actor, grace):
        return await actor.drain(grace)

    def no_watch(_on_change, _on_error):
        class Handle:
            def unsubscribe(self):
                return None

        return Handle()

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=factory,
        control_actor_factory=factory.control_only,
        subscribe=no_watch,
        worker_directory=directory,
        drain_actor=drain,
    )
    factory.on_rebound = worker.rebind_actor
    allocator_loop = ControlAllocatorLoop(
        conversations,
        directory,
        FirestoreAllocator(client, collection_prefix=prefix),
        clock,
        allocator_id="control-a",
    )

    async def outputs():
        records = []
        async for snapshot in client.collection(f"{prefix}_outbox").stream():
            records.append(snapshot.to_dict())
        return sorted(records, key=lambda item: item["sequence"])

    try:
        assert await directory.register(
            WorkerRecord("worker-a", "pod-a:boot-1", 2, 0, 0, False, datetime.now(UTC))
        )
        first = await inbox.accept_input(
            conversation_key=key,
            input_id="input-1",
            conversation_generation=1,
            kind="message",
            payload={"text": "hello", "user": "U1", "team": "T", "channel": "C", "sender": SENDER},
            created_at=datetime.now(UTC),
        )
        assert await allocator_loop.run_once() == 1
        assert await worker.run_once() == 1
        token = (await ownership.get(key)) or pytest.fail("ownership missing")

        async def input_completed():
            record = await inbox.list_unfinished(key)
            return not record

        await _until(input_completed)
        records = await _until(lambda: _turn_finished(outputs))
        sequences = [record["sequence"] for record in records]
        assert sequences == sorted(set(sequences))
        assert {record["payload"]["kind"] for record in records} >= {"TurnStarted", "TurnFinished"}
        state = await conversations.get_conversation(key) or {}
        assert state["turn_status"] == "idle"
        assert factory.identity  # adapter available for the Sandbox identity provider
        identity = await factory.identity(key)
        assert identity["owner_epoch"] == token.epoch
        assert identity["origin_seq"] >= first.seq

        # STOP: authorized by Control, applied by the Worker, then closed.
        stop = await inbox.accept_control(
            conversation_key=key,
            input_id="stop-1",
            conversation_generation=1,
            kind="stop_candidate",
            sender_id="U1",
            payload={"text": "stop", "sender": SENDER},
            created_at=datetime.now(UTC),
        )
        assert await inbox.resolve_control_authorization(
            conversation_key=key, input_id=stop.input_id, authorized=True, now=datetime.now(UTC)
        )

        async def stop_handled():
            return not await inbox.list_authorized_controls(key)

        await _until(stop_handled)
        control = await client.collection(f"{prefix}_control_queue").get()
        assert any(doc.to_dict()["authorization_state"] == "handled" for doc in control)

        # RESET: generation advances while the same actor keeps the assignment.
        reset = await inbox.accept_control(
            conversation_key=key,
            input_id="reset-1",
            conversation_generation=1,
            kind="reset_candidate",
            sender_id="U1",
            payload={"text": "reset", "sender": SENDER},
            created_at=datetime.now(UTC),
        )
        assert await inbox.resolve_control_authorization(
            conversation_key=key, input_id=reset.input_id, authorized=True, now=datetime.now(UTC)
        )

        async def reset_done():
            current = await ownership.get(key)
            return current is not None and current.conversation_generation == 2

        await _until(reset_done)
        await worker.run_once()
        assert worker.actors == (key,)
        current = await ownership.get(key)
        assert current is not None and factory.actors[key].owner == current

        after = await inbox.accept_input(
            conversation_key=key,
            input_id="input-2",
            conversation_generation=2,
            kind="message",
            payload={"text": "again", "user": "U1", "team": "T", "channel": "C", "sender": SENDER},
            created_at=datetime.now(UTC),
        )

        async def second_completed():
            return not [
                item for item in await inbox.list_unfinished(key) if item.input_id == after.input_id
            ]

        await _until(second_completed)
        final = await outputs()
        assert [r["sequence"] for r in final] == sorted({r["sequence"] for r in final})
        assert {r["conversation_generation"] for r in final} == {1, 2}
    finally:
        await worker.aclose()
        client.close()


async def _turn_finished(outputs):
    records = await outputs()
    return records if any(r["payload"]["kind"] == "TurnFinished" for r in records) else None


@pytest.mark.asyncio
async def test_expired_ownership_is_recovered_only_with_worker_stop_evidence():
    from datetime import timedelta

    from silk_agent.distributed.control_runtime import RecoveryLoop
    from silk_agent.distributed.quiesce import QuiesceRequester, QuiesceResponder
    from silk_agent.distributed.recovery import DistributedRecoveryCoordinator
    from silk_agent.stores.firestore.quiesce import FirestoreQuiesceMailbox

    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    dist = DistributedExecutionConfig(firestore_project=f"silk-agent-test-{uuid.uuid4().hex[:12]}")
    client = create_firestore_client(dist, emulator_host=emulator_host)
    clock = SystemClock()
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    ownership = FirestoreOwnershipStore(client, collection_prefix=prefix)
    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    mailbox = FirestoreQuiesceMailbox(client, collection_prefix=prefix)
    key = f"slack:T:C:{uuid.uuid4().hex}"

    class Actor:
        def __init__(self, token):
            self.closed = 0

        async def start(self):
            return None

        async def update_ownership(self, token):
            return None

        async def aclose(self):
            self.closed += 1

    def no_watch(_on_change, _on_error):
        class Handle:
            def unsubscribe(self):
                return None

        return Handle()

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=Actor,
        subscribe=no_watch,
    )
    recovery = RecoveryLoop(
        ownership,
        DistributedRecoveryCoordinator(ownership, clock),
        QuiesceRequester(mailbox, clock, wait_seconds=0.6, poll_seconds=0.05),
        clock,
        skew_seconds=0,
    )
    allocator_loop = ControlAllocatorLoop(
        conversations,
        directory,
        FirestoreAllocator(client, collection_prefix=prefix),
        clock,
        allocator_id="control-a",
    )
    try:
        await directory.register(
            WorkerRecord("worker-a", "pod-a:boot-1", 2, 0, 0, False, datetime.now(UTC))
        )
        await inbox.accept_input(
            conversation_key=key,
            input_id="input-1",
            conversation_generation=1,
            kind="message",
            payload={"text": "hello", "user": "U1"},
            created_at=datetime.now(UTC),
        )
        assert await allocator_loop.run_once() == 1
        assert await worker.run_once() == 1
        old = await ownership.get(key)
        assert old is not None

        # Lease passes while the Worker is unreachable: no evidence, so nothing is released.
        owner_doc = client.collection(f"{prefix}_ownership").document(
            (await client.collection(f"{prefix}_ownership").get())[0].id
        )
        await owner_doc.update(
            {"expires_at": (datetime.now(UTC) - timedelta(seconds=30)).isoformat()}
        )
        assert await recovery.run_once() == 0
        conversation = await conversations.get_conversation(key) or {}
        assert conversation["allocation_state"] == "recovery_blocked"

        # The Worker process is back, closes the stale actor and answers the quiesce request.
        responder = QuiesceResponder(
            mailbox, worker, clock, worker_id="worker-a", incarnation="pod-a:boot-1"
        )
        await worker.run_once()  # sees the expired lease and closes its actor (records the epoch)
        recovered, answered = await asyncio.gather(recovery.run_once(), _answer_soon(responder))
        assert answered >= 1 and recovered == 1
        conversation = await conversations.get_conversation(key) or {}
        assert conversation["allocation_state"] == "waiting"
        assert await ownership.get(key) is None
    finally:
        await worker.aclose()
        client.close()


async def _answer_soon(responder):
    total = 0
    for _ in range(20):
        await asyncio.sleep(0.05)
        total += await responder.run_once()
    return total
