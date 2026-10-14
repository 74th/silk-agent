from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.core.background import CommandCancelReport
from silk_agent.core.distributed_models import InputDeliveryState, OwnershipToken
from silk_agent.distributed.stop import DistributedStopCoordinator
from tests.fakes.distributed_inbox import FakeInboxStore


class Conversations:
    def __init__(self, data):
        self.data = data

    async def get_conversation(self, key):
        return self.data.get(key)


@pytest.mark.asyncio
async def test_authorized_stop_barrier_cancels_prior_queue_but_preserves_later_input():
    inbox = FakeInboxStore()
    now = datetime(2026, 10, 14, tzinfo=UTC)
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    inbox.owners[owner.conversation_key] = owner
    old = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="before-stop",
        conversation_generation=1,
        kind="message",
        payload={"text": "A"},
        created_at=now,
    )
    control = await inbox.accept_control(
        conversation_key=owner.conversation_key,
        input_id="stop",
        conversation_generation=1,
        kind="stop_candidate",
        sender_id="U1",
        payload={"text": "STOP"},
        created_at=now,
    )
    later = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="after-stop",
        conversation_generation=1,
        kind="message",
        payload={"text": "B"},
        created_at=now,
    )
    assert control.seq == 2 and later.seq == 3
    assert await inbox.resolve_control_authorization(
        conversation_key=owner.conversation_key,
        input_id="stop",
        authorized=True,
        now=now,
    )
    assert not await inbox.advance_input(
        conversation_key=owner.conversation_key,
        input_id=old.input_id,
        owner=owner,
        expected=InputDeliveryState.PENDING,
        new_state=InputDeliveryState.DISPATCHING,
        dispatch_attempt_id="late-attempt",
    )
    interrupt_started = asyncio.Event()
    cancel_started = asyncio.Event()
    interrupt_release = asyncio.Event()

    async def interrupt():
        interrupt_started.set()
        await interrupt_release.wait()

    async def cancel_commands():
        cancel_started.set()
        return CommandCancelReport(requested=1, unknown=1)

    coordinator = DistributedStopCoordinator(
        inbox,
        Conversations(inbox.conversations),
        owner,
        interrupt_turn=interrupt,
        cancel_commands=cancel_commands,
        page_size=1,
    )
    stopping = asyncio.create_task(coordinator.stop())
    await asyncio.gather(interrupt_started.wait(), cancel_started.wait())
    interrupt_release.set()
    summary = await stopping
    assert summary.inputs_cancelled == 1 and summary.interrupt_requested
    assert summary.command_cancellation.unknown == 1
    assert (
        inbox.inputs[(owner.conversation_key, "before-stop")].delivery_state
        is InputDeliveryState.CANCELLED
    )
    assert (
        inbox.inputs[(owner.conversation_key, "after-stop")].delivery_state
        is InputDeliveryState.PENDING
    )


@pytest.mark.asyncio
async def test_background_only_stop_does_not_require_or_create_an_sdk_actor():
    inbox = FakeInboxStore()
    now = datetime(2026, 10, 14, tzinfo=UTC)
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    inbox.owners[owner.conversation_key] = owner
    inbox.conversations[owner.conversation_key] = {"stop_seq": 0, "turn_status": "idle"}
    cancelled = []

    async def cancel_commands():
        cancelled.append("cancelled")

    coordinator = DistributedStopCoordinator(
        inbox,
        Conversations(inbox.conversations),
        owner,
        cancel_commands=cancel_commands,
    )
    result = await coordinator.stop()
    assert result.inputs_cancelled == 0
    assert not result.interrupt_requested
    assert cancelled == ["cancelled"]
