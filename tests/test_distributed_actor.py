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
from silk_agent.testing.fakes import AllowAllGuard, FakeAgentEngine, NoopSanitizer, Text
from tests.fakes.distributed_inbox import FakeInboxStore


async def wait_for_state(inbox, key, input_id, expected, seconds=3.0):
    """The completed record is written after the TurnFinished event (the stores yield)."""
    deadline = asyncio.get_running_loop().time() + seconds
    while inbox.inputs[(key, input_id)].delivery_state is not expected:
        assert asyncio.get_running_loop().time() < deadline, "state was not reached"
        await asyncio.sleep(0.01)


SENDER = {"email": "u@example.com", "is_guest": False, "is_external": False, "team": "T"}


@pytest.mark.asyncio
async def test_distributed_actor_dispatches_durable_input_and_completes_it(clock, channel):
    owner = OwnershipToken(
        "T:C:thread",
        "worker-0",
        "inc-1",
        1,
        1,
        clock.now() + timedelta(minutes=1),
    )
    inbox = FakeInboxStore()
    inbox.owners[owner.conversation_key] = owner
    envelope = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="input-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "hello", "user": "U1", "team": "T", "channel": "C", "sender": SENDER},
        created_at=clock.now(),
    )
    orchestrator = ConversationOrchestrator(
        lambda _key: FakeAgentEngine(clock, [Text("reply")]),
        channel,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(()),
        settings=SessionSettings(),
    )
    actor = DistributedConversationActor(
        owner,
        inbox,
        orchestrator,
        read_controls=_no_controls,
        handle_control=_ignore_control,
    )

    await actor.start()
    await channel.wait_for(TurnFinished)

    await wait_for_state(
        inbox, owner.conversation_key, envelope.input_id, InputDeliveryState.COMPLETED
    )
    await actor.aclose()


@pytest.mark.asyncio
async def test_actor_start_finishes_a_reset_rebind_the_previous_owner_left_half_done(
    clock, channel
):
    # A reset advanced the conversation to generation 2 and stopped before it moved the input
    # that arrived after it; the new owner (generation 2) finishes the job when it starts.
    owner = OwnershipToken(
        "T:C:thread", "worker-1", "inc-2", 2, 2, clock.now() + timedelta(minutes=1)
    )
    inbox = FakeInboxStore()
    inbox.owners[owner.conversation_key] = owner
    envelope = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="after-reset",
        conversation_generation=1,  # still the old generation
        kind="message",
        payload={"text": "keep me", "user": "U1", "team": "T", "channel": "C", "sender": SENDER},
        created_at=clock.now(),
    )
    inbox.rebind_markers = {owner.conversation_key: (1, 0)}
    orchestrator = ConversationOrchestrator(
        lambda _key: FakeAgentEngine(clock, [Text("reply")]),
        channel,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(()),
        settings=SessionSettings(),
    )
    actor = DistributedConversationActor(
        owner,
        inbox,
        orchestrator,
        read_controls=_no_controls,
        handle_control=_ignore_control,
    )
    await actor.start()
    key = (owner.conversation_key, envelope.input_id)
    assert inbox.inputs[key].conversation_generation == 2
    assert await inbox.pending_rebind(owner.conversation_key) is None
    await channel.wait_for(TurnFinished)  # and it is processed
    await actor.aclose()


@pytest.mark.asyncio
async def test_a_stale_rebind_marker_of_an_earlier_reset_is_left_alone(clock, channel):
    owner = OwnershipToken(
        "T:C:thread", "worker-1", "inc-2", 5, 3, clock.now() + timedelta(minutes=1)
    )
    inbox = FakeInboxStore()
    inbox.owners[owner.conversation_key] = owner
    inbox.rebind_markers = {owner.conversation_key: (1, 0)}  # generation 3 != 1 + 1
    orchestrator = ConversationOrchestrator(
        lambda _key: FakeAgentEngine(clock, [Text("reply")]),
        channel,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(()),
        settings=SessionSettings(),
    )
    actor = DistributedConversationActor(
        owner,
        inbox,
        orchestrator,
        read_controls=_no_controls,
        handle_control=_ignore_control,
    )
    await actor.start()  # must not raise (the fake rejects a mismatched generation)
    assert await inbox.pending_rebind(owner.conversation_key) == (1, 0)
    await actor.aclose()


@pytest.mark.asyncio
async def test_distributed_actor_resumes_queued_input_without_rechecking_guard(clock, channel):
    owner = OwnershipToken(
        "T:C:thread",
        "worker-0",
        "inc-1",
        1,
        1,
        clock.now() + timedelta(minutes=1),
    )
    inbox = FakeInboxStore()
    inbox.owners[owner.conversation_key] = owner
    envelope = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="queued-input",
        conversation_generation=1,
        kind="message",
        payload={"text": "resume", "user": "U1", "sender": SENDER},
        created_at=clock.now(),
    )
    inbox.inputs[(owner.conversation_key, envelope.input_id)] = replace(
        envelope, delivery_state=InputDeliveryState.QUEUED
    )
    guard_calls = []

    class Guard:
        async def check(self, text, history):
            guard_calls.append(text)
            return None

    orchestrator = ConversationOrchestrator(
        lambda _key: FakeAgentEngine(clock, [Text("reply")]),
        channel,
        clock,
        guard=Guard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(()),
        settings=SessionSettings(),
    )
    actor = DistributedConversationActor(
        owner,
        inbox,
        orchestrator,
        read_controls=_no_controls,
        handle_control=_ignore_control,
    )

    await actor.start()
    await channel.wait_for(TurnFinished)

    assert guard_calls == []
    await wait_for_state(
        inbox, owner.conversation_key, envelope.input_id, InputDeliveryState.COMPLETED
    )
    await actor.aclose()


async def _no_controls():
    return ()


async def _ignore_control(_control):
    return None


@pytest.mark.asyncio
async def test_a_message_waits_for_its_sender_and_is_then_judged_with_it(clock, channel):
    from silk_agent.config import FilterConfig
    from silk_agent.core.filters import default_chain

    owner = OwnershipToken("T:C:thread", "w", "inc", 1, 1, clock.now() + timedelta(minutes=1))
    inbox = FakeInboxStore()
    inbox.owners[owner.conversation_key] = owner
    envelope = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="input-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "hello", "user": "U1", "team": "T", "channel": "C"},  # sender pending
        created_at=clock.now(),
    )
    chain = default_chain(clock, deny_guests=FilterConfig(deny_guests=True).deny_guests)
    orchestrator = ConversationOrchestrator(
        lambda _key: FakeAgentEngine(clock, [Text("reply")]),
        channel,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(()),
        filters=chain,
        settings=SessionSettings(),
    )
    actor = DistributedConversationActor(
        owner,
        inbox,
        orchestrator,
        read_controls=_no_controls,
        handle_control=_ignore_control,
        poll_seconds=0.02,
    )
    await actor.start()
    await asyncio.sleep(0.2)
    # Not judged yet: it neither ran nor was blocked by a missing (unclassified) sender.
    assert inbox.inputs[(owner.conversation_key, envelope.input_id)].delivery_state is (
        InputDeliveryState.PENDING
    )
    inbox.inputs[(owner.conversation_key, envelope.input_id)] = replace(
        envelope, payload={**envelope.payload, "sender": SENDER}
    )
    await channel.wait_for(TurnFinished)
    await wait_for_state(
        inbox, owner.conversation_key, envelope.input_id, InputDeliveryState.COMPLETED
    )
    await actor.aclose()


@pytest.mark.asyncio
async def test_held_results_are_released_even_if_the_reply_finishes_first(clock, channel):
    """The release runs on the actor's task, not the input's, which is cancelled once the
    input completes (it leaves the unfinished set)."""
    owner = OwnershipToken("T:C:thread", "w", "inc", 1, 1, clock.now() + timedelta(minutes=5))
    gate = asyncio.Event()

    class SlowHeldInbox(FakeInboxStore):
        async def list_held(self, conversation_key, *, limit=50):
            await gate.wait()  # the held search outlives the user's reply
            return await super().list_held(conversation_key, limit=limit)

    inbox = SlowHeldInbox()
    inbox.owners[owner.conversation_key] = owner
    held = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="held-1",
        conversation_generation=1,
        kind="command_finished",
        payload={"command_id": "cmd-1", "state": "finished"},
        created_at=clock.now(),
    )
    inbox.inputs[(owner.conversation_key, held.input_id)] = replace(
        held, delivery_state=InputDeliveryState.HELD
    )
    await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="resume-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "続けて", "user": "U1", "team": "T", "channel": "C", "sender": SENDER},
        created_at=clock.now(),
    )
    orchestrator = ConversationOrchestrator(
        lambda _key: FakeAgentEngine(clock, [Text("reply")]),
        channel,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(()),
        settings=SessionSettings(),
    )
    actor = DistributedConversationActor(
        owner,
        inbox,
        orchestrator,
        read_controls=_no_controls,
        handle_control=_ignore_control,
        poll_seconds=0.02,
    )
    await actor.start()
    await channel.wait_for(TurnFinished)
    await asyncio.sleep(0.3)  # the resume input completes and the dispatcher task is cancelled
    assert inbox.inputs[(owner.conversation_key, "resume-1")].delivery_state is (
        InputDeliveryState.COMPLETED
    )
    gate.set()  # now the held search finishes
    for _ in range(100):
        if inbox.inputs[(owner.conversation_key, held.input_id)].delivery_state is not (
            InputDeliveryState.HELD
        ):
            break
        await asyncio.sleep(0.02)
    assert inbox.inputs[(owner.conversation_key, held.input_id)].delivery_state is not (
        InputDeliveryState.HELD
    )
    await actor.aclose()
