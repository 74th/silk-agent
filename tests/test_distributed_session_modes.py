from __future__ import annotations

from datetime import timedelta

import pytest

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.events import TurnFinished, TurnInterrupted, TurnStarted, UserMessage
from silk_agent.core.session import AgentSessionRegistry, SessionSettings
from silk_agent.distributed.input_delivery import DurableInputDelivery
from silk_agent.testing.fakes import FakeAgentEngine, Text, Tool
from tests.fakes.distributed_inbox import FakeInboxStore


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["append", "interrupt", "queue"])
async def test_distributed_delivery_preserves_session_modes(mode, clock, channel):
    inbox = FakeInboxStore()
    owner = OwnershipToken(
        "conversation",
        "worker",
        "incarnation",
        1,
        1,
        clock.now() + timedelta(minutes=1),
    )
    inbox.owners[owner.conversation_key] = owner
    first = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="input-a",
        conversation_generation=1,
        kind="message",
        payload={"text": "A"},
        created_at=clock.now(),
    )
    second = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="input-b",
        conversation_generation=1,
        kind="message",
        payload={"text": "B"},
        created_at=clock.now(),
    )
    holder = {}
    cancelled_commands = []
    registry = AgentSessionRegistry(
        lambda _key: FakeAgentEngine(
            clock, lambda request: [Tool("bash", duration=5), Text(request.text)]
        ),
        channel.send,
        clock,
        SessionSettings(interrupt_mode=mode),
        cancel_commands=lambda _key: _append(cancelled_commands, _key),
        input_started=lambda input_id: holder["delivery"].mark_started(input_id),
        input_completed=lambda input_id: _await_result(holder["delivery"].complete(input_id)),
        input_uncertain=lambda input_id: _await_result(holder["delivery"].mark_uncertain(input_id)),
    )
    session = await registry.get_or_create(owner.conversation_key)
    delivery = DurableInputDelivery(inbox, owner, session)
    holder["delivery"] = delivery
    assert await delivery.dispatch(first, UserMessage(owner.conversation_key, "U", "A")) == "queued"
    await channel.wait_for(TurnStarted)
    outcome = await delivery.dispatch(second, UserMessage(owner.conversation_key, "U", "B"))

    if mode == "append":
        assert outcome == "appended"
        await clock.advance(5)
        await channel.wait_for(TurnFinished)
        assert inbox.inputs[(owner.conversation_key, "input-b")].delivery_state.value == "completed"
    elif mode == "interrupt":
        assert outcome == "queued"
        await channel.wait_for(TurnStarted, count=2)
        assert channel.of(TurnInterrupted)[0].reason == "new_message"
        await clock.advance(5)
        await channel.wait_for(TurnFinished)
        assert inbox.inputs[(owner.conversation_key, "input-b")].delivery_state.value == "completed"
    else:
        assert outcome == "queued"
        await clock.advance(5)
        await channel.wait_for(TurnFinished)
        await channel.wait_for(TurnStarted, count=2)
        await clock.advance(5)
        await channel.wait_for(TurnFinished, count=2)
        assert inbox.inputs[(owner.conversation_key, "input-b")].delivery_state.value == "completed"
    assert cancelled_commands == []
    await registry.aclose()


async def _append(values, value):
    values.append(value)


async def _await_result(awaitable):
    await awaitable
