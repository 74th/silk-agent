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


def build(clock, channel, inbox, scenarios, *, mode="append", **orchestrator_kwargs):
    engines = []

    def engine_factory(_key):
        engine = FakeAgentEngine(clock, scenarios)
        engines.append(engine)
        return engine

    orchestrator = ConversationOrchestrator(
        engine_factory,
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
        read_controls=_no_controls,
        handle_control=_ignore,
        poll_seconds=0.02,
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
    assert state(inbox, "fresh") is States.COMPLETED
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
