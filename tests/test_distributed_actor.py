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

    assert inbox.inputs[(owner.conversation_key, envelope.input_id)].delivery_state is (
        InputDeliveryState.COMPLETED
    )
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
    assert inbox.inputs[(owner.conversation_key, envelope.input_id)].delivery_state is (
        InputDeliveryState.COMPLETED
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
    assert inbox.inputs[(owner.conversation_key, envelope.input_id)].delivery_state is (
        InputDeliveryState.COMPLETED
    )
    await actor.aclose()
