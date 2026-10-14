from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.core.distributed_models import InputDeliveryState, OwnershipToken
from silk_agent.distributed.completion import CompletionInputDelivery
from tests.fakes.distributed_inbox import FakeInboxStore


class Session:
    def __init__(self, outcome):
        self.outcome = outcome
        self.text = None
        self.input_id = None

    async def notify(self, text, *, input_id=""):
        self.text = text
        self.input_id = input_id
        return self.outcome


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "state"),
    [
        ("queued", InputDeliveryState.QUEUED),
        ("appended", InputDeliveryState.ACCEPTED),
        ("held", InputDeliveryState.HELD),
    ],
)
async def test_completion_input_is_durably_attached_to_session(outcome, state):
    inbox = FakeInboxStore()
    now = datetime.now(UTC)
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    inbox.owners[owner.conversation_key] = owner
    envelope = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="completion-1",
        conversation_generation=1,
        kind="command_finished",
        payload={"command_id": "cmd-1", "state": "finished", "result": {"output": {}}},
        created_at=now,
    )
    session = Session(outcome)
    assert await CompletionInputDelivery(inbox, owner, session).dispatch(envelope) == outcome
    assert session.input_id == envelope.input_id
    assert "cmd-1" in session.text
    assert inbox.inputs[(owner.conversation_key, envelope.input_id)].delivery_state is state


@pytest.mark.asyncio
async def test_completion_delivery_after_owner_release_never_starts_an_sdk_turn():
    inbox = FakeInboxStore()
    now = datetime.now(UTC)
    owner = OwnershipToken("conversation", "worker-a", "inc-a", 1, 1, now + timedelta(minutes=1))
    inbox.owners[owner.conversation_key] = owner
    envelope = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="completion-after-release",
        conversation_generation=1,
        kind="command_finished",
        payload={"command_id": "cmd-1", "state": "finished"},
        created_at=now,
    )
    replacement_owner = OwnershipToken(
        owner.conversation_key, "worker-b", "inc-b", 2, 1, now + timedelta(minutes=1)
    )
    inbox.owners[owner.conversation_key] = replacement_owner
    session = Session("queued")
    assert (
        await CompletionInputDelivery(inbox, owner, session).dispatch(envelope)
        == "claimed_elsewhere"
    )
    assert session.input_id is None
    assert (
        inbox.inputs[(owner.conversation_key, envelope.input_id)].delivery_state
        is InputDeliveryState.PENDING
    )

    replacement_session = Session("queued")
    assert (
        await CompletionInputDelivery(inbox, replacement_owner, replacement_session).dispatch(
            envelope
        )
        == "queued"
    )
    assert replacement_session.input_id == envelope.input_id
    assert (
        inbox.inputs[(owner.conversation_key, envelope.input_id)].delivery_state
        is InputDeliveryState.QUEUED
    )
