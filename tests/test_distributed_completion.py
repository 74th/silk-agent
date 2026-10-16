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


@pytest.mark.asyncio
async def test_queued_completion_left_by_a_previous_worker_is_handed_to_the_new_session():
    from dataclasses import replace

    inbox = FakeInboxStore()
    now = datetime.now(UTC)
    owner = OwnershipToken("conversation", "worker-b", "inc-b", 2, 1, now + timedelta(minutes=1))
    inbox.owners[owner.conversation_key] = owner
    envelope = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="completion-queued",
        conversation_generation=1,
        kind="command_finished",
        payload={"command_id": "cmd-1", "state": "finished"},
        created_at=now,
    )
    queued = replace(envelope, delivery_state=InputDeliveryState.QUEUED)
    inbox.inputs[(owner.conversation_key, envelope.input_id)] = queued
    session = Session("appended")
    assert await CompletionInputDelivery(inbox, owner, session).dispatch(queued) == "appended"
    assert session.input_id == envelope.input_id  # notified exactly once, not dropped
    assert inbox.inputs[(owner.conversation_key, envelope.input_id)].delivery_state is (
        InputDeliveryState.ACCEPTED
    )
    # accepted/uncertain results are never re-sent.
    accepted = inbox.inputs[(owner.conversation_key, envelope.input_id)]
    session.input_id = None
    assert (
        await CompletionInputDelivery(inbox, owner, session).dispatch(accepted)
        == "claimed_elsewhere"
    )
    assert session.input_id is None


@pytest.mark.asyncio
async def test_user_resume_releases_held_results_back_to_the_queue_once():
    from silk_agent.distributed.completion import release_held_completions

    inbox = FakeInboxStore()
    now = datetime.now(UTC)
    owner = OwnershipToken("conversation", "worker", "inc", 1, 1, now + timedelta(minutes=1))
    inbox.owners[owner.conversation_key] = owner
    envelope = await inbox.accept_input(
        conversation_key=owner.conversation_key,
        input_id="completion-held",
        conversation_generation=1,
        kind="command_finished",
        payload={"command_id": "cmd-1", "state": "finished"},
        created_at=now,
    )
    session = Session("held")
    await CompletionInputDelivery(inbox, owner, session).dispatch(envelope)
    key = (owner.conversation_key, envelope.input_id)
    assert inbox.inputs[key].delivery_state is InputDeliveryState.HELD
    # While held it is not offered to the actor (an interrupted turn is never resumed alone).
    assert envelope.input_id not in {
        i.input_id for i in await inbox.list_unfinished("conversation")
    }

    assert await release_held_completions(inbox, owner, command_monitor=None, now=now) == 1
    assert inbox.inputs[key].delivery_state is InputDeliveryState.QUEUED
    assert await release_held_completions(inbox, owner, command_monitor=None, now=now) == 0

    resumed = Session("appended")
    queued = inbox.inputs[key]
    assert await CompletionInputDelivery(inbox, owner, resumed).dispatch(queued) == "appended"
    assert "cmd-1" in resumed.text
