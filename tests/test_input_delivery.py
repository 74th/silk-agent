from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.core.distributed_models import InputDeliveryState, OwnershipToken
from silk_agent.core.events import UserMessage
from silk_agent.distributed.input_delivery import DurableInputDelivery
from tests.fakes.distributed_inbox import FakeInboxStore


class Session:
    def __init__(self, outcome):
        self.outcome = outcome
        self.message = None

    async def submit(self, message, *, before_queue=None):
        self.message = message
        if self.outcome == "raise":
            raise OSError("query outcome unknown")
        if self.outcome == "queued" and before_queue is not None:
            if not await before_queue(message.input_id):
                return "rejected"
        return self.outcome


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        ("queued", InputDeliveryState.QUEUED),
        ("appended", InputDeliveryState.ACCEPTED),
    ],
)
async def test_durable_handoff_separates_memory_queue_and_sdk_acceptance(outcome, expected):
    inbox = FakeInboxStore()
    now = datetime(2026, 10, 14, tzinfo=UTC)
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    inbox.owners[owner.conversation_key] = owner
    envelope = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="message-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "hello"},
        created_at=now,
    )
    session = Session(outcome)
    delivery = DurableInputDelivery(inbox, owner, session)
    message = UserMessage(owner.conversation_key, "U1", "hello")
    assert await delivery.dispatch(envelope, message) == outcome
    assert session.message.input_id == "message-1"
    saved = inbox.inputs[(owner.conversation_key, "message-1")]
    assert saved.delivery_state is expected
    if outcome == "queued":
        unfinished = await inbox.list_unfinished(owner.conversation_key)
        assert len(unfinished) == 1 and unfinished[0].delivery_state is InputDeliveryState.QUEUED
        assert await delivery.mark_started("message-1")
        saved = inbox.inputs[(owner.conversation_key, "message-1")]
        assert saved.delivery_state is InputDeliveryState.ACCEPTED
    assert await delivery.complete("message-1")
    assert (
        inbox.inputs[(owner.conversation_key, "message-1")].delivery_state
        is InputDeliveryState.COMPLETED
    )


@pytest.mark.asyncio
async def test_unknown_session_handoff_stays_dispatching_for_uncertain_recovery():
    inbox = FakeInboxStore()
    now = datetime(2026, 10, 14, tzinfo=UTC)
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    inbox.owners[owner.conversation_key] = owner
    envelope = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="message-unknown",
        conversation_generation=1,
        kind="message",
        payload={"text": "hello"},
        created_at=now,
    )
    delivery = DurableInputDelivery(inbox, owner, Session("raise"))
    with pytest.raises(OSError, match="unknown"):
        await delivery.dispatch(envelope, UserMessage(owner.conversation_key, "U1", "hello"))
    saved = inbox.inputs[(owner.conversation_key, "message-unknown")]
    assert saved.delivery_state is InputDeliveryState.DISPATCHING
    replacement = OwnershipToken(
        owner.conversation_key,
        "worker-b",
        "incarnation-b",
        2,
        1,
        now + timedelta(minutes=2),
    )
    inbox.owners[owner.conversation_key] = replacement
    assert await inbox.recover_dispatching(
        conversation_key=owner.conversation_key,
        input_id="message-unknown",
        previous_owner_epoch=owner.epoch,
        dispatch_attempt_id=saved.dispatch_attempt_id,
    )
    assert (
        inbox.inputs[(owner.conversation_key, "message-unknown")].delivery_state
        is InputDeliveryState.UNCERTAIN
    )


@pytest.mark.asyncio
async def test_two_workers_sharing_inbox_cannot_dispatch_the_same_input():
    inbox = FakeInboxStore()
    now = datetime(2026, 10, 14, tzinfo=UTC)
    owner_a = OwnershipToken(
        "conversation", "worker-a", "incarnation-a", 1, 1, now + timedelta(minutes=1)
    )
    owner_b = OwnershipToken(
        "conversation", "worker-b", "incarnation-b", 2, 1, now + timedelta(minutes=1)
    )
    inbox.owners[owner_a.conversation_key] = owner_a
    envelope = await inbox.accept_input(
        conversation_key=owner_a.conversation_key,
        input_id="shared-message",
        conversation_generation=1,
        kind="message",
        payload={"text": "hello"},
        created_at=now,
    )
    session_a = Session("queued")
    session_b = Session("queued")
    delivery_a = DurableInputDelivery(inbox, owner_a, session_a)
    delivery_b = DurableInputDelivery(inbox, owner_b, session_b)
    results = await asyncio.gather(
        delivery_a.dispatch(envelope, UserMessage(owner_a.conversation_key, "U1", "hello")),
        delivery_b.dispatch(envelope, UserMessage(owner_b.conversation_key, "U1", "hello")),
    )
    assert results.count("queued") == 1
    assert results.count("rejected") == 1
    assert sum(session.message is not None for session in (session_a, session_b)) == 1


@pytest.mark.asyncio
async def test_delivery_uses_the_current_token_after_a_lease_renewal_mid_operation():
    from dataclasses import replace as dc_replace
    from datetime import timedelta

    inbox = FakeInboxStore()
    now = datetime.now(UTC)
    first = OwnershipToken("conversation", "w", "i", 1, 1, now + timedelta(seconds=60))
    inbox.owners[first.conversation_key] = first
    envelope = await inbox.accept_input(
        conversation_key=first.conversation_key,
        input_id="in-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "hi", "user": "U1"},
        created_at=now,
    )
    holder = {"token": first}

    class Session:
        async def submit(self, message, *, before_queue=None):
            # The Worker renews its lease between the first CAS and the hand-off.
            renewed = dc_replace(first, expires_at=first.expires_at + timedelta(seconds=20))
            inbox.owners[first.conversation_key] = renewed
            holder["token"] = renewed
            assert before_queue is not None and await before_queue(message.input_id)
            return "queued"

    delivery = DurableInputDelivery(inbox, lambda: holder["token"], Session())
    message = UserMessage("conversation", "U1", "hi", input_id="in-1")
    assert await delivery.dispatch(envelope, message) == "queued"
