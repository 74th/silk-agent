import json
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.config import DistributedExecutionConfig
from silk_agent.core.distributed_models import WorkerRecord
from silk_agent.distributed.control_runtime import RecoveryLoop
from silk_agent.stores.firestore.allocator import FirestoreAllocator
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.inbox import FirestoreInboxStore
from silk_agent.stores.firestore.ownership import FirestoreOwnershipStore
from silk_agent.stores.firestore.workers import FirestoreWorkerDirectory

pytestmark = pytest.mark.integration


class Env:
    def __init__(self):
        host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
        if not host:
            pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
        project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
        self.prefix = f"test-{uuid.uuid4().hex[:12]}"
        self.client = create_firestore_client(
            DistributedExecutionConfig(firestore_project=project), emulator_host=host
        )
        self.inbox = FirestoreInboxStore(self.client, collection_prefix=self.prefix)
        self.directory = FirestoreWorkerDirectory(self.client, collection_prefix=self.prefix)
        self.allocator = FirestoreAllocator(self.client, collection_prefix=self.prefix)
        self.ownership = FirestoreOwnershipStore(self.client, collection_prefix=self.prefix)
        self.now = datetime.now(UTC)

    async def assign(
        self, key: str, worker: WorkerRecord, *, activate: bool, now: datetime | None = None
    ):
        now = now or self.now
        await self.inbox.accept_input(
            conversation_key=key,
            input_id=f"input-{key}",
            conversation_generation=1,
            kind="message",
            payload={"text": "hello"},
            created_at=now,
        )
        token = await self.allocator.allocate(
            allocator_id="test",
            conversation_key=key,
            candidates=(worker,),
            now=now,
            allocator_lease_expires_at=now + timedelta(seconds=30),
            worker_lease_expires_at=now + timedelta(seconds=5),
        )
        assert token is not None
        if activate:
            assert await self.ownership.activate(token, now=now)
        return token

    async def counters(self, worker_id: str) -> tuple[int, int] | None:
        snapshot = await self.directory._worker_ref(worker_id).get()
        if not snapshot.exists:
            return None
        return snapshot.get("reserved"), snapshot.get("active_sdk")

    async def block(self, token):
        expired = self.now + timedelta(seconds=6)
        assert await self.ownership.begin_recovery(token, now=expired)
        assert await self.ownership.mark_recovery_blocked(token, reason="test", now=expired)
        return expired


async def test_a_manual_release_leaves_the_counters_of_a_newer_incarnation_alone():
    env = Env()
    try:
        old_worker = WorkerRecord("worker-a", "incarnation-old", 4, 0, 0, False, env.now)
        await env.directory.register(old_worker)
        old = await env.assign("conversation-restart", old_worker, activate=True)
        expired = await env.block(old)
        # The container restarted: same worker id, new incarnation, counters start from zero.
        new_worker = WorkerRecord("worker-a", "incarnation-new", 4, 0, 0, False, expired)
        assert await env.directory.register(new_worker)
        other = await env.assign("conversation-other", new_worker, activate=True, now=expired)
        assert other is not None
        assert await env.counters("worker-a") == (1, 1)

        assert await env.ownership.confirm_stopped(
            old,
            evidence_id="manual",
            worker_incarnation="incarnation-old",
            now=expired + timedelta(seconds=1),
        )
        assert await env.counters("worker-a") == (1, 1)  # the new incarnation's reservation
    finally:
        env.client.close()


async def test_ownership_is_released_after_the_worker_record_expired():
    env = Env()
    try:
        worker = WorkerRecord("worker-b", "incarnation-b", 4, 0, 0, False, env.now)
        await env.directory.register(worker)
        token = await env.assign("conversation-ttl", worker, activate=True)
        expired = await env.block(token)
        await env.directory._worker_ref("worker-b").delete()  # removed by its TTL

        assert await env.ownership.confirm_stopped(
            token,
            evidence_id="manual",
            worker_incarnation="incarnation-b",
            now=expired + timedelta(seconds=1),
        )
        assert await env.counters("worker-b") is None  # nothing was recreated
    finally:
        env.client.close()


async def test_an_unactivated_assignment_is_released_after_the_worker_record_expired():
    env = Env()
    try:
        worker = WorkerRecord("worker-c", "incarnation-c", 4, 0, 0, False, env.now)
        await env.directory.register(worker)
        token = await env.assign("conversation-unactivated", worker, activate=False)
        await env.directory._worker_ref("worker-c").delete()
        assert await env.ownership.release_unactivated(token, now=env.now + timedelta(seconds=6))
    finally:
        env.client.close()


async def test_a_restarted_container_is_recovered_automatically():
    env = Env()
    try:
        old_incarnation = json.dumps(["pod-uid-1", "boot-1"])
        old_worker = WorkerRecord("worker-d", old_incarnation, 4, 0, 0, False, env.now)
        await env.directory.register(old_worker)
        old = await env.assign("conversation-oom", old_worker, activate=True)
        later = env.now + timedelta(seconds=600)
        # OOM kill: same Pod, same UID, a new incarnation registers under the same worker id.
        new_worker = WorkerRecord(
            "worker-d", json.dumps(["pod-uid-1", "boot-2"]), 4, 0, 0, False, later
        )
        assert await env.directory.register(new_worker)

        class Blocked:
            state = "blocked"

        class Coordinator:
            async def recover(self, token, *, request_quiesce):
                assert await env.ownership.begin_recovery(token, now=later)
                await env.ownership.mark_recovery_blocked(token, reason="no answer", now=later)
                return Blocked()

        class Clock:
            def now(self):
                return later

        async def current_incarnation(worker_id):
            worker = await env.directory.get_worker(worker_id)
            return worker.incarnation if worker is not None else None

        recovery = RecoveryLoop(
            env.ownership,
            Coordinator(),
            object(),
            Clock(),
            current_incarnation=current_incarnation,
            auto_recover_after_seconds=120,
        )
        assert await recovery.run_once() == 1
        assert not await env.ownership.list_expired(before=later, limit=10)  # released
        assert await env.counters("worker-d") == (0, 0)  # the new incarnation's slots untouched
        assert old is not None
    finally:
        env.client.close()


async def test_list_expired_page_covers_every_expired_ownership_in_pages():
    env = Env()
    try:
        expired_keys = set()
        for number in range(5):
            worker = WorkerRecord(f"worker-p{number}", f"inc-p{number}", 4, 0, 0, False, env.now)
            await env.directory.register(worker)
            key = f"conversation-page-{number}"
            await env.assign(key, worker, activate=True)
            expired_keys.add(key)
        later = env.now + timedelta(seconds=60)
        seen, cursor = set(), None
        for _ in range(10):
            page, cursor = await env.ownership.list_expired_page(
                before=later, limit=2, after=cursor
            )
            assert len(page) <= 2
            seen.update(token.conversation_key for token in page)
            if cursor is None:
                break
        assert cursor is None and seen == expired_keys
    finally:
        env.client.close()


async def test_the_allocator_picks_the_worker_with_the_fewest_reservations():
    env = Env()
    try:
        busy = WorkerRecord("worker-busy", "inc-busy", 4, 0, 0, False, env.now)
        idle = WorkerRecord("worker-idle", "inc-idle", 4, 0, 0, False, env.now)
        await env.directory.register(busy)
        await env.directory.register(idle)
        # Three conversations land on "busy" first (it is the only candidate offered).
        for number in range(3):
            await env.assign(f"conversation-fill-{number}", busy, activate=False)
        busy_now = await env.directory.get_worker("worker-busy")
        idle_now = await env.directory.get_worker("worker-idle")
        assert (busy_now.reserved, idle_now.reserved) == (3, 0)
        # With both offered, busy first, the new conversation goes to the idle one.
        await env.inbox.accept_input(
            conversation_key="conversation-new",
            input_id="input-new",
            conversation_generation=1,
            kind="message",
            payload={"text": "hello"},
            created_at=env.now,
        )
        token = await env.allocator.allocate(
            allocator_id="test",
            conversation_key="conversation-new",
            candidates=(busy_now, idle_now),
            now=env.now,
            allocator_lease_expires_at=env.now + timedelta(seconds=30),
            worker_lease_expires_at=env.now + timedelta(seconds=5),
        )
        assert token is not None and token.worker_id == "worker-idle"
    finally:
        env.client.close()


async def test_holds_ownership_tells_a_renewed_owner_from_a_fenced_one():
    from silk_agent.stores.firestore.conversations import FirestoreConversationStore

    env = Env()
    try:
        conversations = FirestoreConversationStore(env.client, collection_prefix=env.prefix)
        worker = WorkerRecord("worker-h", "inc-h", 4, 0, 0, False, env.now)
        await env.directory.register(worker)
        token = await env.assign("conversation-holds", worker, activate=True)
        assert await conversations.holds_ownership(token)
        # A renewal changes only the expiry: the old token is still the holder.
        renewed = await env.ownership.renew(
            token, now=env.now, lease_expires_at=env.now + timedelta(minutes=30)
        )
        assert renewed is not None and await conversations.holds_ownership(token)
        # Another epoch took over: the old writer is fenced.
        taken = token.__class__(
            token.conversation_key,
            token.worker_id,
            token.worker_incarnation,
            token.epoch + 1,
            token.conversation_generation,
            token.expires_at + timedelta(minutes=30),
        )
        from silk_agent.stores.firestore.codec import encode_record

        owner_ref = env.inbox._collection("ownership").document(
            env.inbox._conversation_id("conversation-holds")
        )
        await owner_ref.set({**encode_record(taken), "phase": "active"})
        assert not await conversations.holds_ownership(token)
        assert await conversations.holds_ownership(taken)
    finally:
        env.client.close()
