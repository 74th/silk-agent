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
    assert worker.usage == (2, 2)

    del ownership.tokens["conversation-a"]
    assert await worker.run_once() == 1
    assert actors["conversation-a"].closed == 1
    assert worker.actors == ("conversation-b",)


@pytest.mark.asyncio
async def test_worker_releases_the_slot_when_the_actor_cannot_be_built():
    ownership = Ownership([token("conversation-a")])

    def actor_factory(owner):
        raise FileNotFoundError("prompt.md")

    worker = DistributedWorker(
        ownership,
        Clock(),
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=actor_factory,
        subscribe=no_watch,
    )
    with pytest.raises(FileNotFoundError):
        await worker.run_once()
    assert ownership.released == ["conversation-a"]


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
    assert worker.usage == (1, 0)


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
    # This process closed that epoch itself, so a repeated request is still provable ...
    assert (await worker.quiesce(owner)) is not None
    # ... but an epoch it never ran, or another incarnation, is not.
    unknown_epoch = OwnershipToken(
        owner.conversation_key,
        owner.worker_id,
        owner.worker_incarnation,
        owner.epoch + 1,
        owner.conversation_generation,
        owner.expires_at,
    )
    assert await worker.quiesce(unknown_epoch) is None
    other = OwnershipToken(
        owner.conversation_key,
        owner.worker_id,
        "pod-a:boot-2",
        owner.epoch,
        owner.conversation_generation,
        owner.expires_at,
    )
    assert await worker.quiesce(other) is None


async def _resolved(value):
    return value


@pytest.mark.asyncio
async def test_worker_undrain_resumes_assignment_processing():
    clock = Clock()
    ownership = Ownership([])
    directory = FakeSharedStore()
    await directory.register(WorkerRecord("worker-a", "pod-a:boot-1", 2, 0, 0, True, clock.now()))
    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=Actor,
        subscribe=no_watch,
        worker_directory=directory,
    )
    assert await worker.run_once() == 0
    await directory.set_draining(
        worker_id="worker-a", incarnation="pod-a:boot-1", draining=False, now=clock.now()
    )
    ownership.tokens["new-conversation"] = token("new-conversation")
    assert await worker.run_once() == 1


def _unavailable_worker(clock):
    class Unavailable(Ownership):
        async def renew(self, *args, **kwargs):
            raise ConnectionError("store offline")

    ownership = Unavailable([token("conversation-a")])
    actors = []
    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=lambda owner: actors.append(Actor(owner)) or actors[-1],
        subscribe=no_watch,
    )
    return ownership, actors, worker


@pytest.mark.asyncio
async def test_a_transient_renewal_error_keeps_the_actor_while_the_lease_lasts():
    # 60 s lease, renewed every 20 s: after one failed call 40 s remain, so the next renewal
    # still comes in time. Closing now would interrupt the conversation for a blip.
    clock = Clock()
    ownership, actors, worker = _unavailable_worker(clock)
    await worker.run_once()
    await worker._renew_actors()
    assert actors[0].closed == 0
    assert worker.actors == ("conversation-a",)
    assert ownership.released == []


@pytest.mark.asyncio
async def test_renewal_errors_close_the_actor_when_the_lease_is_about_to_end():
    clock = Clock()
    ownership, actors, worker = _unavailable_worker(clock)
    await worker.run_once()
    clock.value += timedelta(seconds=45)  # 15 s left: less than one renewal interval
    await worker._renew_actors()
    assert actors[0].closed == 1
    assert worker.actors == ()
    assert ownership.released == []  # never released without proof the SDK stopped


@pytest.mark.asyncio
async def test_long_drain_keeps_renewing_lease_and_releases_latest_token():
    import asyncio

    clock = Clock()
    ownership = Ownership([token("conversation-a")])
    directory = FakeSharedStore()
    await directory.register(WorkerRecord("worker-a", "pod-a:boot-1", 2, 0, 0, False, clock.now()))
    renewed = asyncio.Event()
    original_renew = ownership.renew

    async def renew(*args, **kwargs):
        value = await original_renew(*args, **kwargs)
        renewed.set()
        return value

    ownership.renew = renew

    async def drain(actor, grace):
        await renewed.wait()
        await actor.aclose()
        return True

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=Actor,
        subscribe=no_watch,
        worker_directory=directory,
        drain_actor=drain,
        renew_seconds=0.01,
    )
    await worker.run_once()
    await directory.set_draining(
        worker_id="worker-a", incarnation="pod-a:boot-1", draining=True, now=clock.now()
    )
    assert await asyncio.wait_for(worker.run_once(), timeout=1) == 0
    assert renewed.is_set()
    assert ownership.released == ["conversation-a"]


@pytest.mark.asyncio
async def test_continuous_watch_signals_do_not_starve_lease_renewal():
    import asyncio

    clock = Clock()
    ownership = Ownership([token("conversation-a")])
    stop = asyncio.Event()
    original = ownership.renew
    calls = []

    async def renew(*args, **kwargs):
        calls.append(True)
        stop.set()
        return await original(*args, **kwargs)

    ownership.renew = renew

    def busy_watch(on_change, on_error):
        async def send_signals():
            while not stop.is_set():
                on_change()
                await asyncio.sleep(0)

        task = asyncio.create_task(send_signals())

        class Handle:
            def unsubscribe(self):
                task.cancel()

        return Handle()

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=Actor,
        subscribe=busy_watch,
        renew_seconds=0.01,
    )
    await asyncio.wait_for(worker.run(stop), timeout=1)
    assert calls


@pytest.mark.asyncio
async def test_cancelling_assignment_loop_closes_actors_before_returning():
    import asyncio

    actors = []
    started = asyncio.Event()

    class StartedActor(Actor):
        async def start(self):
            await super().start()
            started.set()

    ownership = Ownership([token("conversation-a")])
    worker = DistributedWorker(
        ownership,
        Clock(),
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=lambda owner: actors.append(StartedActor(owner)) or actors[-1],
        subscribe=no_watch,
    )
    task = asyncio.create_task(worker.run(asyncio.Event()))
    await started.wait()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert actors[0].closed == 1
    assert ownership.released == ["conversation-a"]


@pytest.mark.asyncio
async def test_all_sdk_actors_start_shutdown_within_one_process_grace_window():
    import asyncio

    stopping = []
    all_stopping = asyncio.Event()

    async def drain(actor, grace):
        stopping.append(actor.token.conversation_key)
        if len(stopping) == 2:
            all_stopping.set()
        await all_stopping.wait()
        await actor.aclose()
        return True

    ownership = Ownership([token("conversation-a"), token("conversation-b")])
    worker = DistributedWorker(
        ownership,
        Clock(),
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=Actor,
        subscribe=no_watch,
        drain_actor=drain,
    )
    await worker.run_once()
    await asyncio.wait_for(worker.aclose(), timeout=1)
    assert set(ownership.released) == {"conversation-a", "conversation-b"}


class IdleActor(Actor):
    def __init__(self, tok):
        super().__init__(tok)
        self.idle = True
        self.rebound = []

    def is_idle(self):
        return self.idle

    async def rebind_after_reset(self, tok):
        self.rebound.append(tok)
        self.token = tok


@pytest.mark.asyncio
async def test_idle_actor_is_closed_then_released_only_after_the_idle_period():
    clock = Clock()
    owner = token("conversation-a")
    ownership = Ownership([owner])
    actors = {}

    def actor_factory(tok):
        actors[tok.conversation_key] = IdleActor(tok)
        return actors[tok.conversation_key]

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=actor_factory,
        subscribe=no_watch,
        idle_close_seconds=600,
        lease_seconds=3600,
        renew_seconds=20,
    )
    await worker.run_once()
    await worker._renew_actors()
    assert worker.actors == ("conversation-a",)

    clock.value += timedelta(seconds=599)
    await worker._renew_actors()
    assert ownership.released == [] and worker.usage == (1, 1)

    actors["conversation-a"].idle = False  # a new turn resets the idle period
    await worker._renew_actors()
    actors["conversation-a"].idle = True
    await worker._renew_actors()  # idle period starts when idleness is first observed
    clock.value += timedelta(seconds=599)
    await worker._renew_actors()
    assert ownership.released == []

    clock.value += timedelta(seconds=2)
    await worker._renew_actors()
    assert ownership.released == ["conversation-a"]
    assert actors["conversation-a"].closed == 1
    assert worker.actors == () and worker.usage == (0, 0)


@pytest.mark.asyncio
async def test_actor_survives_its_own_reset_successor_but_not_a_foreign_takeover():
    clock = Clock()
    owner = token("conversation-a")
    ownership = Ownership([owner])
    actors = {}

    def actor_factory(tok):
        actors[tok.conversation_key] = IdleActor(tok)
        return actors[tok.conversation_key]

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=actor_factory,
        subscribe=no_watch,
    )
    await worker.run_once()
    successor = OwnershipToken("conversation-a", "worker-a", "pod-a:boot-1", 2, 2, owner.expires_at)
    ownership.tokens["conversation-a"] = successor
    assert await worker.run_once() == 1
    actor = actors["conversation-a"]
    assert actor.rebound == [successor] and actor.closed == 0

    foreign = OwnershipToken("conversation-a", "worker-a", "pod-a:boot-1", 4, 3, owner.expires_at)
    ownership.tokens["conversation-a"] = foreign
    await worker.run_once()
    assert actor.closed == 1 and worker.actors == ()


@pytest.mark.asyncio
async def test_shutdown_waits_past_the_turn_grace_so_an_interrupted_actor_is_still_released(
    monkeypatch,
):
    import asyncio

    from silk_agent.distributed import worker as worker_module

    monkeypatch.setattr(worker_module, "SIGTERM_DRAIN_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(worker_module, "SIGTERM_STOP_DEADLINE_SECONDS", 1.0)
    clock = Clock()
    ownership = Ownership([token("conversation-a")])

    async def slow_interrupt_and_save(_actor, _grace):
        await asyncio.sleep(0.2)  # longer than the turn grace: the interrupt + save phase
        return True

    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=Actor,
        subscribe=no_watch,
        drain_actor=slow_interrupt_and_save,
    )
    await worker.run_once()
    await worker.aclose()
    assert ownership.released == ["conversation-a"]


@pytest.mark.asyncio
async def test_close_marks_the_worker_draining_before_releasing_any_actor():
    events = []

    class Directory:
        async def set_draining(self, **kwargs):
            events.append(("draining", kwargs["draining"]))

        async def get_worker(self, _worker_id):
            return None

    class RecordingOwnership(Ownership):
        async def release(self, tok, *, now):
            events.append(("release", tok.conversation_key))
            return await super().release(tok, now=now)

    clock = Clock()
    worker = DistributedWorker(
        RecordingOwnership([token("conversation-a")]),
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=Actor,
        subscribe=no_watch,
    )
    await worker.run_once()
    worker._worker_directory = Directory()
    await worker.aclose()
    assert events == [("draining", True), ("release", "conversation-a")]


@pytest.mark.asyncio
async def test_a_silent_watch_costs_one_renewal_interval_not_the_conversation():
    import asyncio

    clock = Clock()
    ownership = Ownership([])  # nothing assigned when the Worker starts
    actors = []
    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=lambda owner: actors.append(Actor(owner)) or actors[-1],
        subscribe=no_watch,  # the listener is dead: it never reports anything
        renew_seconds=0.05,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    await asyncio.sleep(0.02)
    ownership.tokens["conversation-a"] = token("conversation-a")  # assigned; no notification
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert [a.token.conversation_key for a in actors] == ["conversation-a"]
