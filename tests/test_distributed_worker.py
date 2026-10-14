from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.core.distributed_models import OwnershipToken, WorkerRecord
from silk_agent.distributed.worker import DistributedWorker
from tests.fakes.distributed_store import FakeSharedStore


class Clock:
    def __init__(self):
        self.value = datetime(2026, 10, 14, tzinfo=UTC)

    def now(self):
        return self.value

    async def sleep(self, seconds):
        self.value += timedelta(seconds=seconds)


class Ownership:
    def __init__(self, tokens):
        self.tokens = {token.conversation_key: token for token in tokens}
        self.active = set()
        self.released = []
        self.sdk_active = set()

    async def list_for_worker(self, *, worker_id, worker_incarnation):
        return tuple(
            token
            for token in self.tokens.values()
            if token.worker_id == worker_id and token.worker_incarnation == worker_incarnation
        )

    async def activate(self, token, *, now):
        if self.tokens.get(token.conversation_key) != token or token.expires_at <= now:
            return False
        if token.conversation_key in self.active:
            return False
        self.active.add(token.conversation_key)
        self.sdk_active.add(token.conversation_key)
        return True

    async def activate_control(self, token, *, now):
        if self.tokens.get(token.conversation_key) != token or token.expires_at <= now:
            return False
        if token.conversation_key in self.active:
            return False
        self.active.add(token.conversation_key)
        return True

    async def renew(self, token, *, now, lease_expires_at):
        if (
            token.conversation_key not in self.active
            or self.tokens.get(token.conversation_key) != token
        ):
            return None
        renewed = OwnershipToken(
            token.conversation_key,
            token.worker_id,
            token.worker_incarnation,
            token.epoch,
            token.conversation_generation,
            lease_expires_at,
        )
        self.tokens[token.conversation_key] = renewed
        return renewed

    async def release(self, token, *, now):
        self.active.discard(token.conversation_key)
        self.sdk_active.discard(token.conversation_key)
        self.released.append(token.conversation_key)
        return True


class Actor:
    def __init__(self, token):
        self.token = token
        self.started = 0
        self.closed = 0

    async def start(self):
        self.started += 1

    async def update_ownership(self, token):
        self.token = token

    async def aclose(self):
        self.closed += 1


def token(key, *, worker="worker-a", incarnation="pod-a:boot-1"):
    return OwnershipToken(key, worker, incarnation, 1, 1, datetime(2026, 10, 14, 0, 1, tzinfo=UTC))


def no_watch(on_change, on_error):
    class Handle:
        def unsubscribe(self):
            pass

    return Handle()


@pytest.mark.asyncio
async def test_worker_creates_one_actor_per_active_conversation_and_closes_removed_assignment():
    clock = Clock()
    ownership = Ownership([token("conversation-a"), token("conversation-b")])
    actors = {}

    def actor_factory(owner):
        actor = Actor(owner)
        actors[owner.conversation_key] = actor
        return actor

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=actor_factory,
        subscribe=no_watch,
    )
    assert await worker.run_once() == 2
    assert await worker.run_once() == 2
    assert actors["conversation-a"].started == 1
    assert actors["conversation-b"].started == 1

    del ownership.tokens["conversation-a"]
    assert await worker.run_once() == 1
    assert actors["conversation-a"].closed == 1
    assert worker.actors == ("conversation-b",)


@pytest.mark.asyncio
async def test_worker_ignores_expired_or_other_incarnation_assignments():
    clock = Clock()
    ownership = Ownership(
        [
            OwnershipToken(
                "expired",
                "worker-a",
                "pod-a:boot-1",
                1,
                1,
                datetime(2026, 10, 13, tzinfo=UTC),
            ),
            token("other-incarnation", incarnation="pod-a:boot-old"),
            token("other-worker", worker="worker-b"),
        ]
    )
    actors = []
    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=lambda owner: actors.append(Actor(owner)) or actors[-1],
        subscribe=no_watch,
    )
    assert await worker.run_once() == 0
    assert actors == []


@pytest.mark.asyncio
async def test_worker_closes_actor_when_ownership_renewal_is_lost():
    clock = Clock()
    owner = token("conversation-a")

    class LostRenewalOwnership(Ownership):
        async def renew(self, _token, *, now, lease_expires_at):
            del now, lease_expires_at
            return None

    ownership = LostRenewalOwnership([owner])
    actors = []
    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=lambda token: actors.append(Actor(token)) or actors[-1],
        subscribe=no_watch,
    )

    assert await worker.run_once() == 1
    await worker._renew_actors()

    assert actors[0].closed == 1
    assert worker.actors == ()
    assert ownership.released == []


@pytest.mark.asyncio
async def test_control_only_assignment_uses_actor_without_creating_an_sdk_actor():
    clock = Clock()
    ownership = Ownership([token("background-only")])
    sdk_actors = []
    control_actors = []

    def make_control_actor(owner):
        actor = Actor(owner)
        control_actors.append(actor)
        return actor

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=lambda owner: sdk_actors.append(Actor(owner)) or sdk_actors[-1],
        control_actor_factory=make_control_actor,
        is_control_only=lambda _key: _resolved(True),
        subscribe=no_watch,
    )
    assert await worker.run_once() == 1
    assert sdk_actors == []
    assert len(control_actors) == 1 and control_actors[0].started == 1
    assert ownership.sdk_active == set()


@pytest.mark.asyncio
async def test_draining_worker_stops_receiving_reservations_without_losing_usage():
    store = FakeSharedStore()
    now = datetime(2026, 10, 14, tzinfo=UTC)
    record = WorkerRecord("worker-a", "pod-a:boot-1", 2, 1, 1, False, now)
    assert await store.register(record)
    updated = await store.set_draining(
        worker_id=record.worker_id,
        incarnation=record.incarnation,
        draining=True,
        now=now + timedelta(seconds=1),
    )
    assert updated is not None and updated.draining
    assert updated.reserved == record.reserved
    assert updated.active_sdk == record.active_sdk
    assert await store.list_available(now=now + timedelta(seconds=1)) == ()
    assert await store.heartbeat(
        WorkerRecord(
            record.worker_id,
            record.incarnation,
            record.capacity,
            record.reserved,
            record.active_sdk,
            False,
            now + timedelta(seconds=2),
        )
    )
    assert (await store.get_worker(record.worker_id)).draining
    assert (
        await store.set_draining(
            worker_id=record.worker_id,
            incarnation="stale-incarnation",
            draining=False,
            now=now + timedelta(seconds=2),
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("control_only", [False, True])
async def test_distributed_worker_releases_assignments_after_confirmed_drain(control_only):
    clock = Clock()
    owner = token("conversation-a")
    ownership = Ownership([owner])
    directory = FakeSharedStore()
    worker_record = WorkerRecord("worker-a", "pod-a:boot-1", 2, 0, 0, False, clock.now())
    assert await directory.register(worker_record)
    actors = []
    drain_calls = []

    async def drain_actor(actor, grace_seconds):
        drain_calls.append(grace_seconds)
        await actor.aclose()
        return True

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=lambda token: actors.append(Actor(token)) or actors[-1],
        subscribe=no_watch,
        worker_directory=directory,
        drain_actor=drain_actor,
        is_control_only=lambda _key: _resolved(control_only),
        control_actor_factory=lambda token: actors.append(Actor(token)) or actors[-1],
    )
    assert await worker.run_once() == 1
    await directory.set_draining(
        worker_id="worker-a",
        incarnation="pod-a:boot-1",
        draining=True,
        now=clock.now(),
    )
    assert await worker.run_once() == 0
    assert drain_calls == [None]
    assert actors[0].closed == 1
    assert ownership.released == ["conversation-a"]
    assert ownership.sdk_active == set()


@pytest.mark.asyncio
async def test_worker_passes_remaining_operator_drain_time_and_keeps_owner_until_confirmed():
    clock = Clock()
    owner = token("conversation-a")
    ownership = Ownership([owner])
    directory = FakeSharedStore()
    assert await directory.register(
        WorkerRecord("worker-a", "pod-a:boot-1", 2, 0, 0, False, clock.now())
    )
    drain_calls = []

    async def drain_actor(_actor, grace_seconds):
        drain_calls.append(grace_seconds)
        return len(drain_calls) > 1

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=Actor,
        subscribe=no_watch,
        worker_directory=directory,
        drain_actor=drain_actor,
    )
    assert await worker.run_once() == 1
    deadline = clock.now() + timedelta(seconds=30)
    await directory.set_draining(
        worker_id="worker-a",
        incarnation="pod-a:boot-1",
        draining=True,
        now=clock.now(),
        drain_deadline_at=deadline,
    )
    clock.value += timedelta(seconds=10)

    # An unconfirmed drain keeps the owner blocked for a later retry.
    assert await worker.run_once() == 1
    assert drain_calls == [20]
    assert ownership.released == []

    clock.value = deadline + timedelta(seconds=5)
    assert await worker.run_once() == 0
    assert drain_calls == [20, 0.0]
    assert ownership.released == ["conversation-a"]


@pytest.mark.asyncio
async def test_worker_does_not_release_ownership_when_actor_stop_is_unconfirmed():
    clock = Clock()
    ownership = Ownership([token("conversation-a")])

    class FailingCloseActor(Actor):
        async def aclose(self):
            raise RuntimeError("SDK stop was not confirmed")

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=FailingCloseActor,
        subscribe=no_watch,
    )
    assert await worker.run_once() == 1
    await worker.aclose()
    assert ownership.released == []


@pytest.mark.asyncio
async def test_worker_sigterm_fallback_uses_25_second_grace_and_keeps_unconfirmed_owner():
    clock = Clock()
    owner = token("conversation-a")
    ownership = Ownership([owner])
    grace_values = []

    async def drain_actor(_actor, grace_seconds):
        grace_values.append(grace_seconds)
        return False

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=Actor,
        subscribe=no_watch,
        drain_actor=drain_actor,
    )
    assert await worker.run_once() == 1
    await worker.aclose()
    assert grace_values == [25.0]
    assert ownership.released == []


@pytest.mark.asyncio
async def test_worker_quiesce_returns_stop_evidence_without_releasing_owner():
    clock = Clock()
    owner = token("conversation-a")
    ownership = Ownership([owner])
    actors = []
    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=lambda token: actors.append(Actor(token)) or actors[-1],
        subscribe=no_watch,
    )
    assert await worker.run_once() == 1
    evidence = await worker.quiesce(owner)
    assert evidence is not None
    assert evidence["sdk_stopped"] is True
    assert evidence["worker_incarnation"] == owner.worker_incarnation
    assert isinstance(evidence["evidence_id"], str)
    assert actors[0].closed == 1
    assert ownership.released == []
    assert await worker.quiesce(owner) is None


async def _resolved(value):
    return value
