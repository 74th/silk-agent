from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.channels.slack_users import SlackUser
from silk_agent.distributed.enrichment import ControlAuthorizer, SlackSenderEnricher
from tests.fakes.distributed_inbox import FakeInboxStore


class Clock:
    def __init__(self):
        self.value = datetime(2026, 10, 14, tzinfo=UTC)

    def now(self):
        return self.value


class Users:
    def __init__(self, values):
        self.values = list(values)
        self.looked_up = []

    async def lookup(self, user_id):
        self.looked_up.append(user_id)
        return self.values.pop(0)


async def accept(inbox, now):
    return await inbox.accept_received(
        conversation_key="slack:T1:C1:100.1",
        input_id="C1:101.1",
        conversation_generation=1,
        kind="message",
        payload={"text": "hello", "user": "U1"},
        created_at=now,
        workspace="T1",
        team="T1",
        channel="C1",
        message_ts="101.1",
        event_id="Ev1",
    )


@pytest.mark.asyncio
async def test_sender_enrichment_resolves_policy_fields_after_acceptance():
    inbox = FakeInboxStore()
    clock = Clock()
    await accept(inbox, clock.now())
    users = Users([SlackUser("u@example.com", False, "T1", "E1")])
    enricher = SlackSenderEnricher(
        inbox, users, clock, workspace_team="T1", workspace_enterprise="E1"
    )

    assert await enricher.run_once() == 1
    record = inbox.inputs[("slack:T1:C1:100.1", "C1:101.1")]
    assert record.payload["sender"] == {
        "email": "u@example.com",
        "is_guest": False,
        "is_external": False,
        "team": "T1",
    }
    assert (
        inbox.sender_enrichment[(record.conversation_key, record.input_id)]["sender_lookup_state"]
        == "resolved"
    )
    assert users.looked_up == ["U1"]


@pytest.mark.asyncio
async def test_unavailable_user_lookup_stays_durable_and_retries_with_backoff():
    inbox = FakeInboxStore()
    clock = Clock()
    await accept(inbox, clock.now())
    users = Users([None, SlackUser("", False, "T2", "")])
    enricher = SlackSenderEnricher(inbox, users, clock, workspace_team="T1")

    assert await enricher.run_once() == 1
    state = inbox.sender_enrichment[("slack:T1:C1:100.1", "C1:101.1")]
    assert state["sender_lookup_state"] == "pending"
    assert state["next_sender_lookup_at"] == clock.now() + timedelta(seconds=5)
    assert await enricher.run_once() == 0
    clock.value += timedelta(seconds=5)
    assert await enricher.run_once() == 1
    assert state["sender_lookup_state"] == "resolved"
    assert inbox.inputs[("slack:T1:C1:100.1", "C1:101.1")].payload["sender"]["is_external"] is True


@pytest.mark.asyncio
async def test_regular_input_and_stop_candidate_enrichment_run_independently():
    inbox = FakeInboxStore()
    clock = Clock()
    await accept(inbox, clock.now())
    await inbox.accept_received(
        conversation_key="slack:T1:C1:100.1",
        input_id="C1:102.1",
        conversation_generation=1,
        kind="stop_candidate",
        payload={"text": "STOP"},
        created_at=clock.now(),
        workspace="T1",
        team="T1",
        channel="C1",
        message_ts="102.1",
        event_id="Ev2",
        sender_id="U2",
    )
    users = Users(
        [
            SlackUser("u1@example.com", False, "T1", "E1"),
            SlackUser("u2@example.com", False, "T1", "E1"),
        ]
    )
    enricher = SlackSenderEnricher(
        inbox, users, clock, workspace_team="T1", workspace_enterprise="E1"
    )

    assert await enricher.run_once() == 2
    assert set(users.looked_up) == {"U1", "U2"}
    assert (
        inbox.inputs[("slack:T1:C1:100.1", "C1:101.1")].payload["sender"]["email"]
        == "u1@example.com"
    )
    assert (
        inbox.controls[("slack:T1:C1:100.1", "C1:102.1")].payload["sender"]["email"]
        == "u2@example.com"
    )


@pytest.mark.asyncio
async def test_only_authorized_stop_advances_the_persistent_stop_boundary():
    inbox = FakeInboxStore()
    clock = Clock()
    for user_id, input_id, ts in (
        ("U_ALLOWED", "C1:102.1", "102.1"),
        ("U_DENIED", "C1:103.1", "103.1"),
    ):
        await inbox.accept_received(
            conversation_key="slack:T1:C1:100.1",
            input_id=input_id,
            conversation_generation=1,
            kind="stop_candidate",
            payload={"text": "STOP"},
            created_at=clock.now(),
            workspace="T1",
            team="T1",
            channel="C1",
            message_ts=ts,
            event_id=f"Ev-{user_id}",
            sender_id=user_id,
        )
    users = Users(
        [
            SlackUser("allowed@example.com", False, "T1", "E1"),
            SlackUser("denied@example.com", False, "T1", "E1"),
        ]
    )
    enricher = SlackSenderEnricher(
        inbox, users, clock, workspace_team="T1", workspace_enterprise="E1"
    )
    await enricher.run_once()
    authorizer = ControlAuthorizer(
        inbox, clock, lambda record: record.get("sender_id") == "U_ALLOWED"
    )

    assert await authorizer.run_once() == 2
    assert inbox.conversations["slack:T1:C1:100.1"]["stop_seq"] == 1
    assert inbox.control_authorization[("slack:T1:C1:100.1", "C1:102.1")] == "authorized"
    assert inbox.control_authorization[("slack:T1:C1:100.1", "C1:103.1")] == "filtered"


@pytest.mark.asyncio
async def test_a_sender_that_never_resolves_is_given_up_blocking_the_input_and_telling_the_user():
    inbox = FakeInboxStore()
    clock = Clock()
    envelope = await accept(inbox, clock.now())
    told = []

    async def on_gave_up(key, input_id, collection):
        told.append((key, input_id, collection))

    users = Users([None] * 10)
    enricher = SlackSenderEnricher(
        inbox,
        users,
        clock,
        workspace_team="T1",
        max_attempts=3,
        on_gave_up=on_gave_up,
    )
    key = (envelope.conversation_key, envelope.input_id)
    for _ in range(2):  # attempts 1 and 2 reschedule
        await enricher.run_once()
        clock.value += timedelta(hours=1)
        assert inbox.inputs[key].delivery_state.value == "pending"
        assert not told
    await enricher.run_once()  # attempt 3 ends the lookup
    assert inbox.sender_enrichment[key]["sender_lookup_state"] == "failed"
    assert inbox.inputs[key].delivery_state.value == "blocked"
    assert told == [(envelope.conversation_key, envelope.input_id, "inputs")]
    clock.value += timedelta(hours=1)
    assert await enricher.run_once() == 0  # nothing is due any more: no endless retries
    assert len(users.looked_up) == 3


@pytest.mark.asyncio
async def test_an_unresolvable_stop_candidate_is_filtered_not_authorized():
    inbox = FakeInboxStore()
    clock = Clock()
    control = await inbox.accept_received(
        conversation_key="slack:T1:C1:100.1",
        input_id="C1:102.1",
        conversation_generation=1,
        kind="stop_candidate",
        payload={"text": "停止", "user": "U1"},
        created_at=clock.now(),
        workspace="T1",
        team="T1",
        channel="C1",
        message_ts="102.1",
        event_id="Ev2",
        sender_id="U1",
    )
    enricher = SlackSenderEnricher(
        inbox, Users([None] * 5), clock, workspace_team="T1", max_attempts=1
    )
    await enricher.run_once()
    key = (control.conversation_key, control.input_id)
    assert inbox.control_authorization[key] == "filtered"
