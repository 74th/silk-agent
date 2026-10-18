"""Inputs reach a terminal state when a turn is interrupted, fenced, dropped or never prepared.

These use the real Dispatcher, Actor and AgentSession; only the stores and the SDK are fakes.
"""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest

from silk_agent.core.distributed_models import InputDeliveryState, OwnershipToken
from silk_agent.core.events import TurnFinished
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.session import SessionSettings
from silk_agent.core.stopwords import StopWords
from silk_agent.distributed.actor import DistributedConversationActor
from silk_agent.testing.fakes import AllowAllGuard, FakeAgentEngine, NoopSanitizer, Text, Tool
from tests.fakes.distributed_inbox import FakeInboxStore

SENDER = {"email": "u@example.com", "is_guest": False, "is_external": False, "team": "T"}
KEY = "T:C:thread"
States = InputDeliveryState


async def _no_controls():
    return ()


async def _ignore(_control):
    return None


def owner_for(clock):
    return OwnershipToken(KEY, "w", "inc", 1, 1, clock.now() + timedelta(hours=1))


async def add(inbox, clock, input_id, text="hello"):
    return await inbox.accept_input(
        conversation_key=KEY,
        input_id=input_id,
        conversation_generation=1,
        kind="message",
        payload={"text": text, "user": "U1", "team": "T", "channel": "C", "sender": SENDER},
        created_at=clock.now(),
    )


def build(
    clock,
    channel,
    inbox,
    scenarios,
    *,
    mode="append",
    read_controls=_no_controls,
    handle_control=_ignore,
    actor_kwargs=None,
    engine_factory=None,
    **orchestrator_kwargs,
):
    engines = []

    def default_engine_factory(_key):
        engine = FakeAgentEngine(clock, scenarios)
        engines.append(engine)
        return engine

    orchestrator = ConversationOrchestrator(
        engine_factory or default_engine_factory,
        channel,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(()),
        settings=SessionSettings(interrupt_mode=mode),
        **orchestrator_kwargs,
    )
    owner = owner_for(clock)
    inbox.owners[KEY] = owner
    actor = DistributedConversationActor(
        owner,
        inbox,
        orchestrator,
        read_controls=read_controls,
        handle_control=handle_control,
        poll_seconds=0.02,
        **(actor_kwargs or {}),
    )
    return actor, engines


async def until(predicate, seconds=3.0):
    deadline = asyncio.get_running_loop().time() + seconds
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, "condition not reached"
        await asyncio.sleep(0.01)


def state(inbox, input_id):
    return inbox.inputs[(KEY, input_id)].delivery_state


@pytest.mark.asyncio
async def test_inputs_of_an_interrupted_turn_get_a_terminal_state(clock, channel):
    """R1: a stop used to leave the input 'accepted' forever (slot held, page eventually full)."""
    inbox = FakeInboxStore()
    await add(inbox, clock, "stopped")
    actor, engines = build(clock, channel, inbox, lambda _r: [Tool("bash", duration=3600)])
    await actor.start()
    await until(lambda: state(inbox, "stopped") is States.ACCEPTED)
    assert actor.session is not None
    await actor.session.stop()  # user stop
    await until(lambda: state(inbox, "stopped") is not States.ACCEPTED)
    assert state(inbox, "stopped") is States.CANCELLED
    assert not await inbox.list_unfinished(KEY)  # nothing left to hold the conversation
    await actor.aclose()


@pytest.mark.asyncio
async def test_a_system_interrupt_leaves_the_input_unknown_not_resent(clock, channel):
    inbox = FakeInboxStore()
    await add(inbox, clock, "drained")
    actor, _ = build(clock, channel, inbox, lambda _r: [Tool("bash", duration=3600)])
    await actor.start()
    await until(lambda: state(inbox, "drained") is States.ACCEPTED)
    assert actor.session is not None
    await actor.session.aclose(reason="drain")
    await until(lambda: state(inbox, "drained") is not States.ACCEPTED)
    assert state(inbox, "drained") is States.UNCERTAIN
    await actor.aclose()


@pytest.mark.asyncio
async def test_a_new_actor_settles_leftovers_so_old_residue_cannot_fill_the_page(clock, channel):
    """R1 (page): 100 'accepted' leftovers used to hide every newer input."""
    inbox = FakeInboxStore()
    for index in range(100):
        record = await add(inbox, clock, f"old-{index}")
        inbox.inputs[(KEY, record.input_id)] = replace(record, delivery_state=States.ACCEPTED)
    await add(inbox, clock, "fresh", text="new request")
    actor, engines = build(clock, channel, inbox, lambda _r: [Text("ok")])
    await actor.start()
    await channel.wait_for(TurnFinished)
    await until(lambda: state(inbox, "fresh") is States.COMPLETED)
    assert all(state(inbox, f"old-{i}") is States.UNCERTAIN for i in range(100))
    assert [r.text for r in engines[0].requests] == ["new request"]  # leftovers never resent
    await actor.aclose()


@pytest.mark.asyncio
async def test_input_fenced_after_reaching_the_sdk_is_settled_and_never_retried(clock, channel):
    """R2: a refused 'accepted' record after append used to retry forever behind the input."""
    calls = {"fenced": 0}

    class FencedInbox(FakeInboxStore):
        async def advance_input(self, **kwargs):
            if kwargs["input_id"] == "B" and kwargs["new_state"] is States.ACCEPTED:
                calls["fenced"] += 1
                return False  # a stop boundary landed between the append and this write
            return await super().advance_input(**kwargs)

    inbox = FencedInbox()
    await add(inbox, clock, "A", text="long")
    actor, engines = build(clock, channel, inbox, lambda r: [Tool("bash", duration=3600)])
    await actor.start()
    await until(lambda: state(inbox, "A") is States.ACCEPTED)
    await add(inbox, clock, "B", text="more")  # appended into the running turn
    await until(lambda: state(inbox, "B") is States.UNCERTAIN)
    await asyncio.sleep(0.3)
    assert calls["fenced"] == 1  # one refusal, no retry loop
    assert engines[0].appended == ["more"]  # and the SDK got it exactly once
    await add(inbox, clock, "C", text="later")  # the conversation is not stuck behind B
    await until(lambda: state(inbox, "C") is not States.PENDING)
    await actor.aclose()


@pytest.mark.asyncio
async def test_input_behind_the_stop_boundary_survives_a_stop_that_empties_the_queue(
    clock, channel
):
    """R3: a stop discarded queued inputs from memory; behind the boundary they never ran."""
    inbox = FakeInboxStore()
    await add(inbox, clock, "A", text="long")
    seen = []

    def scenario(request):
        seen.append(request.text)
        return [Tool("bash", duration=3600)] if request.text == "long" else [Text("B done")]

    actor, _ = build(clock, channel, inbox, scenario, mode="queue")
    await actor.start()
    await until(lambda: state(inbox, "A") is States.ACCEPTED)
    b = await add(inbox, clock, "B", text="after the stop")
    await until(lambda: state(inbox, "B") is States.QUEUED)  # waiting in memory behind turn A
    # An authorized stop covers A only (B has a later seq) and empties the memory queue.
    inbox.conversations.setdefault(KEY, {})["stop_seq"] = b.seq - 1
    assert actor.session is not None
    await actor.session.stop()
    await until(lambda: state(inbox, "B") is States.COMPLETED, seconds=5)
    assert seen == ["long", "after the stop"]  # B ran once, after the stop
    assert state(inbox, "A") is States.CANCELLED
    await actor.aclose()


@pytest.mark.asyncio
async def test_a_transient_preparation_failure_does_not_let_a_later_input_overtake(clock, channel):
    """R4: A's failed preparation used to free its turn, so B reached the SDK first."""
    attempts = {"A": 0}

    async def flaky_rate_limit(message):
        if message.text == "A-text":
            attempts["A"] += 1
            if attempts["A"] == 1:
                raise ConnectionError("firestore blip")
        return True

    inbox = FakeInboxStore()
    await add(inbox, clock, "A", text="A-text")
    await add(inbox, clock, "B", text="B-text")
    actor, engines = build(
        clock, channel, inbox, lambda _r: [Text("ok")], distributed_rate_limit=flaky_rate_limit
    )
    await actor.start()
    await until(
        lambda: state(inbox, "B") is States.COMPLETED and state(inbox, "A") is States.COMPLETED
    )
    # A started the turn and B was appended to it. Before the fix B reached the SDK first.
    assert [r.text for r in engines[0].requests] == ["A-text"]
    assert engines[0].appended == ["B-text"]
    assert attempts["A"] == 2
    await actor.aclose()


@pytest.mark.asyncio
async def test_a_poison_input_is_abandoned_so_it_cannot_block_the_conversation(clock, channel):
    async def always_fails(_message):
        raise ValueError("cannot be prepared")

    inbox = FakeInboxStore()
    await add(inbox, clock, "poison", text="poison")
    actor, _ = build(
        clock, channel, inbox, lambda _r: [Text("ok")], distributed_rate_limit=always_fails
    )
    actor._max_prepare_attempts = 2  # fast test; the dispatcher default is 12
    actor.__dict__["_poll_seconds"] = 0.01
    await actor.start()
    assert actor._dispatcher is not None
    actor._dispatcher._max_prepare_attempts = 2
    actor._dispatcher._retry_cap_seconds = 0.02
    await until(lambda: state(inbox, "poison") is States.BLOCKED, seconds=5)
    await actor.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("poll_seconds", [0.02, 5.0])
async def test_two_back_to_back_messages_both_reach_the_sdk(clock, channel, poll_seconds):
    """S1: the reader used to cancel a hand-off in flight, stranding the second message.

    The stores yield on every call (as real RPCs do). With a long poll interval the only scan is
    the wake that follows the first input's hand-off, which is exactly when the second one is
    being handed over.
    """
    inbox = FakeInboxStore()
    await add(inbox, clock, "A", text="first")
    await add(inbox, clock, "B", text="second")
    actor, engines = build(clock, channel, inbox, lambda _r: [Tool("bash", duration=1)])
    actor._poll_seconds = poll_seconds
    await actor.start()
    await until(
        lambda: state(inbox, "A") is not States.PENDING and state(inbox, "B") is not States.PENDING
    )
    await until(lambda: state(inbox, "B") in (States.ACCEPTED, States.QUEUED, States.COMPLETED))
    delivered = [r.text for r in engines[0].requests] + engines[0].appended
    assert sorted(delivered) == ["first", "second"]  # nothing stranded in 'dispatching'
    assert States.DISPATCHING not in {state(inbox, "A"), state(inbox, "B")}
    await actor.aclose()


@pytest.mark.asyncio
async def test_input_arriving_between_stop_authorization_and_application_runs_after_the_stop(
    clock, channel
):
    """S2: with the default append mode, an input that arrived after the stop boundary was
    committed but before the Worker applied it was appended to the turn being stopped, and was
    then cancelled with it."""
    inbox = FakeInboxStore()
    await add(inbox, clock, "A", text="long")
    seen = []

    def scenario(request):
        seen.append(request.text)
        return [Tool("bash", duration=3600)] if request.text == "long" else [Text("B done")]

    apply_stop = asyncio.Event()
    holder = {}

    async def read_controls():
        return await inbox.list_authorized_controls(KEY)

    async def handle_control(control):
        await apply_stop.wait()
        await holder["actor"].session.stop()
        await inbox.mark_control_handled(
            conversation_key=KEY,
            input_id=control["input_id"],
            owner=holder["actor"].owner,
            now=clock.now(),
        )

    async def stop_boundary():
        return inbox.conversations.get(KEY, {}).get("stop_seq", 0)

    actor, engines = build(
        clock,
        channel,
        inbox,
        scenario,
        read_controls=read_controls,
        handle_control=handle_control,
        actor_kwargs={"read_stop_seq": stop_boundary},
    )
    holder["actor"] = actor
    await actor.start()
    await until(lambda: state(inbox, "A") is States.ACCEPTED)
    # Control authorizes a stop (stop_seq committed); the Worker has not applied it yet.
    control = await inbox.accept_control(
        conversation_key=KEY,
        input_id="stop-1",
        conversation_generation=1,
        kind="stop_candidate",
        sender_id="U1",
        payload={"text": "停止", "user": "U1", "sender": SENDER},
        created_at=clock.now(),
    )
    assert await inbox.resolve_control_authorization(
        conversation_key=KEY, input_id="stop-1", authorized=True, now=clock.now()
    )
    b = await add(inbox, clock, "B", text="after the stop")
    assert b.seq > control.seq
    await asyncio.sleep(0.3)
    # It must not be appended to the turn that is about to be stopped.
    assert state(inbox, "B") is States.PENDING
    assert engines[0].appended == []
    apply_stop.set()
    await until(lambda: state(inbox, "B") is States.COMPLETED, seconds=5)
    assert seen == ["long", "after the stop"]  # B ran once, in its own turn, after the stop
    assert state(inbox, "A") is States.CANCELLED
    await actor.aclose()


@pytest.mark.asyncio
async def test_a_stream_that_just_ends_leaves_the_input_unknown_not_cancelled(clock, channel):
    """S4: an SDK stream that ends without a result or an interrupt is not 'the user cancelled'."""

    class EndsAbruptly:
        session_id = None

        async def run_turn(self, request):
            if False:
                yield  # an async generator that yields nothing and just ends

        async def append(self, _text):
            return False

        async def interrupt(self):
            return None

        async def reset(self):
            return None

        async def close(self):
            return None

    inbox = FakeInboxStore()
    await add(inbox, clock, "A", text="hello")
    actor, _ = build(clock, channel, inbox, None, engine_factory=lambda _key: EndsAbruptly())
    await actor.start()
    await until(
        lambda: state(inbox, "A") is not States.ACCEPTED and state(inbox, "A") is not States.PENDING
    )
    await until(lambda: state(inbox, "A") is States.UNCERTAIN)
    await actor.aclose()


@pytest.mark.asyncio
async def test_a_failing_abandon_keeps_the_turn_so_later_inputs_do_not_overtake(clock, channel):
    """S5: abandoning a poison input must itself succeed before the turn is released."""
    abandon_calls = {"count": 0}

    class FlakyAbandon(FakeInboxStore):
        async def advance_input(self, **kwargs):
            if kwargs["input_id"] == "poison" and kwargs["new_state"] is States.BLOCKED:
                abandon_calls["count"] += 1
                if abandon_calls["count"] == 1:
                    raise ConnectionError("firestore blip")
            return await super().advance_input(**kwargs)

    async def always_fails(message):
        if message.text == "poison":
            raise ValueError("cannot be prepared")
        return True

    inbox = FlakyAbandon()
    await add(inbox, clock, "poison", text="poison")
    await add(inbox, clock, "next", text="next")
    actor, engines = build(
        clock, channel, inbox, lambda _r: [Text("ok")], distributed_rate_limit=always_fails
    )
    await actor.start()
    assert actor._dispatcher is not None
    actor._dispatcher._max_prepare_attempts = 2
    actor._dispatcher._retry_cap_seconds = 0.02
    await until(lambda: state(inbox, "next") is States.COMPLETED, seconds=5)
    assert state(inbox, "poison") is States.BLOCKED and abandon_calls["count"] == 2
    assert [r.text for r in engines[0].requests] == ["next"]
    await actor.aclose()


@pytest.mark.asyncio
async def test_a_control_that_keeps_failing_does_not_spin_the_control_loop(clock, channel):
    """T1: a failing control used to wake the reader at once, ~90k Firestore reads per second."""
    inbox = FakeInboxStore()
    reads = {"count": 0}
    calls = {"count": 0}

    async def read_controls():
        reads["count"] += 1
        return ({"input_id": "stop-1", "kind": "stop_candidate", "seq": 1},)

    async def handle_control(_control):
        calls["count"] += 1
        raise ConnectionError("sandbox service is down")

    actor, _ = build(
        clock,
        channel,
        inbox,
        lambda _r: [Text("ok")],
        read_controls=read_controls,
        handle_control=handle_control,
    )
    await actor.start()
    await asyncio.sleep(1.0)
    # poll is 0.02s: a few dozen reads at most, not tens of thousands
    assert calls["count"] < 30 and reads["count"] < 80, (calls, reads)
    await actor.aclose()


@pytest.mark.asyncio
async def test_a_stop_that_cannot_be_applied_delays_only_the_first_input(clock, channel):
    """T2: later inputs must not each wait the whole gate again for the same stuck control."""
    inbox = FakeInboxStore()
    queries = {"count": 0}
    original = inbox.list_authorized_controls

    async def counting(*args, **kwargs):
        queries["count"] += 1
        return await original(*args, **kwargs)

    inbox.list_authorized_controls = counting

    async def handle_control(_control):
        raise ConnectionError("cannot apply the stop")

    async def read_controls():
        return ()

    actor, _ = build(
        clock,
        channel,
        inbox,
        lambda _r: [Text("ok")],
        read_controls=read_controls,
        handle_control=handle_control,
        actor_kwargs={"stop_gate_seconds": 0.4},
    )
    await inbox.accept_control(
        conversation_key=KEY,
        input_id="stop-1",
        conversation_generation=1,
        kind="stop_candidate",
        sender_id="U1",
        payload={"text": "停止", "user": "U1", "sender": SENDER},
        created_at=clock.now(),
    )
    await inbox.resolve_control_authorization(
        conversation_key=KEY, input_id="stop-1", authorized=True, now=clock.now()
    )
    await add(inbox, clock, "B")
    await add(inbox, clock, "C")
    await actor.start()
    await until(lambda: state(inbox, "B") is States.COMPLETED, seconds=5)
    started = asyncio.get_running_loop().time()
    await until(lambda: state(inbox, "C") is States.COMPLETED, seconds=5)
    assert asyncio.get_running_loop().time() - started < 0.3  # C did not wait 0.4s again
    assert queries["count"] < 20  # polled at the recheck interval, not every 50ms
    await actor.aclose()


@pytest.mark.asyncio
async def test_input_sequence_bookkeeping_is_released_once_an_input_is_settled(clock, channel):
    """T5: the actor must not remember every input it ever handled."""
    inbox = FakeInboxStore()
    await add(inbox, clock, "A")
    actor, _ = build(clock, channel, inbox, lambda _r: [Text("ok")])
    await actor.start()
    await until(lambda: state(inbox, "A") is States.COMPLETED)
    await until(lambda: not actor._input_seqs)
    await actor.aclose()


class _HookedEngine:
    """Calls ``on_query_start`` before the prompt, as ClaudeAgentEngine does; failure ends the
    turn without the prompt being sent."""

    def __init__(self, inner, key, hook):
        self._inner = inner
        self._key = key
        self._hook = hook

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def run_turn(self, request):
        from silk_agent.core.events import TurnFailed

        try:
            await self._hook(self._key, "sess-1", request.input_id, request.dispatch_attempt_id)
        except Exception as exc:
            yield TurnFailed(self._key, request.turn_id, f"{type(exc).__name__}: {exc}")
            return
        async for event in self._inner.run_turn(request):
            yield event


def _hooked(clock, inbox, holder, engines):
    from silk_agent.distributed.worker_role import query_start_recorder

    hook = query_start_recorder(inbox, clock, lambda: holder["actor"].owner)

    def factory(key):
        inner = FakeAgentEngine(clock, [Text("ok")])
        engines.append(inner)
        return _HookedEngine(inner, key, hook)

    return factory


@pytest.mark.asyncio
async def test_query_start_is_recorded_for_a_real_turn_after_the_input_is_accepted(clock, channel):
    """8.5: at the moment the turn starts the input is 'accepted', so the association is stored
    and the turn runs."""
    inbox = FakeInboxStore()
    await add(inbox, clock, "A", text="hello")
    holder, engines = {}, []
    actor, _ = build(
        clock, channel, inbox, None, engine_factory=_hooked(clock, inbox, holder, engines)
    )
    holder["actor"] = actor
    await actor.start()
    await until(lambda: state(inbox, "A") is States.COMPLETED)
    payload = inbox.inputs[(KEY, "A")].payload
    assert payload["sdk_session_id"] == "sess-1" and "query_started_at" in payload
    assert [r.text for r in engines[0].requests] == ["hello"]
    await actor.aclose()


@pytest.mark.asyncio
async def test_a_refused_query_start_sends_no_prompt_and_leaves_the_input_unknown(clock, channel):
    """8.5: if the association cannot be stored (the input is no longer ours) the prompt must
    not reach the SDK, and the input must not be resent."""

    class Refusing(FakeInboxStore):
        async def record_query_start(self, **_kwargs):
            return False

    inbox = Refusing()
    await add(inbox, clock, "A", text="hello")
    holder, engines = {}, []
    actor, _ = build(
        clock, channel, inbox, None, engine_factory=_hooked(clock, inbox, holder, engines)
    )
    holder["actor"] = actor
    await actor.start()
    await until(lambda: state(inbox, "A") is States.UNCERTAIN)
    assert all(not engine.requests for engine in engines)
    await actor.aclose()
