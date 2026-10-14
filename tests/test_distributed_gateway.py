from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from silk_agent.distributed.gateway import (
    ReceivedSlackInput,
    SlackEventNormalizer,
    SlackSocketGateway,
)
from tests.fakes.distributed_inbox import FakeInboxStore


class Clock:
    def now(self):
        return datetime(2026, 10, 14, tzinfo=UTC)


class BarrierInbox(FakeInboxStore):
    def __init__(self):
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def accept_received(self, **kwargs):
        self.entered.set()
        await self.release.wait()
        return await super().accept_received(**kwargs)


def sample_input(envelope):
    async def normalize(_envelope):
        return ReceivedSlackInput(
            conversation_key="slack:T:C:123.4",
            input_id="C:123.5",
            kind="message",
            payload={"text": "hello"},
            workspace="W",
            team="T",
            channel="C",
            message_ts="123.5",
            event_id="Ev1",
        )

    return normalize


@pytest.mark.asyncio
async def test_socket_gateway_acknowledges_only_after_inbox_commit():
    inbox = BarrierInbox()
    gateway = SlackSocketGateway(inbox, Clock(), sample_input({}))
    acknowledged = []

    async def ack(response):
        acknowledged.append(response.envelope_id)

    task = asyncio.create_task(
        gateway.handle_message(
            json.dumps({"type": "events_api", "envelope_id": "env-1", "payload": {}}),
            ack,
        )
    )
    await inbox.entered.wait()
    assert acknowledged == []
    inbox.release.set()
    await task
    assert acknowledged == ["env-1"]
    assert len(inbox.inputs) == 1


@pytest.mark.asyncio
async def test_socket_gateway_does_not_ack_when_persistence_fails():
    class BrokenInbox(FakeInboxStore):
        async def accept_received(self, **_kwargs):
            raise OSError("store unavailable")

    gateway = SlackSocketGateway(BrokenInbox(), Clock(), sample_input({}))
    acknowledgements = []

    async def ack(response):
        acknowledgements.append(response.envelope_id)

    with pytest.raises(OSError):
        await gateway.handle_message(
            json.dumps({"type": "events_api", "envelope_id": "env-2", "payload": {}}),
            ack,
        )
    assert acknowledgements == []


@pytest.mark.asyncio
async def test_gateway_starts_sender_enrichment_after_ack_without_waiting_for_lookup():
    inbox = FakeInboxStore()
    lookup_entered = asyncio.Event()
    lookup_release = asyncio.Event()

    async def enrich(_input):
        lookup_entered.set()
        await lookup_release.wait()

    gateway = SlackSocketGateway(inbox, Clock(), sample_input({}), on_accepted=enrich)
    acknowledgements = []

    async def ack(response):
        acknowledgements.append(response.envelope_id)

    await gateway.handle_message(
        json.dumps({"type": "events_api", "envelope_id": "env-3", "payload": {}}),
        ack,
    )
    assert acknowledgements == ["env-3"]
    await lookup_entered.wait()
    assert acknowledgements == ["env-3"]
    lookup_release.set()
    await gateway.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked_phase", ["commit", "ack"])
async def test_gateway_shutdown_waits_for_inflight_commit_and_ack(blocked_phase):
    inbox = BarrierInbox() if blocked_phase == "commit" else FakeInboxStore()
    gateway = SlackSocketGateway(inbox, Clock(), sample_input({}))
    ack_entered = asyncio.Event()
    ack_release = asyncio.Event()
    acknowledged = []

    async def ack(response):
        ack_entered.set()
        await ack_release.wait()
        acknowledged.append(response.envelope_id)

    handler = asyncio.create_task(
        gateway.handle_message(
            json.dumps({"type": "events_api", "envelope_id": f"env-{blocked_phase}"}),
            ack,
        )
    )
    if blocked_phase == "commit":
        await inbox.entered.wait()
    else:
        await ack_entered.wait()

    gateway.stop_accepting()
    shutdown = asyncio.create_task(gateway.aclose())
    await asyncio.sleep(0)
    assert not shutdown.done()
    with pytest.raises(RuntimeError, match="shutting down"):
        await gateway.handle_message("{}", ack)

    if blocked_phase == "commit":
        inbox.release.set()
        await ack_entered.wait()
    ack_release.set()
    await asyncio.gather(handler, shutdown)
    assert acknowledged == [f"env-{blocked_phase}"]
    assert len(inbox.inputs) == 1


def slack_envelope(event_type, event, *, event_id="Ev1"):
    return {
        "type": "events_api",
        "envelope_id": f"env-{event_id}",
        "payload": {
            "type": "event_callback",
            "event_id": event_id,
            "team_id": "T1",
            "event": {"type": event_type, **event},
        },
    }


@pytest.mark.asyncio
async def test_slack_normalizer_preserves_mention_dm_and_participating_thread_rules():
    inbox = FakeInboxStore()
    normalizer = SlackEventNormalizer(
        bot_user_id="UBOT",
        is_participating=lambda key: _participates(inbox, key),
        stop_words=["停止"],
    )
    mention = await normalizer(
        slack_envelope(
            "app_mention",
            {"user": "U1", "text": "<@UBOT> run &amp;&amp;", "channel": "C1", "ts": "100.1"},
        )
    )
    assert mention is not None and mention.mark_participating
    assert mention.conversation_key == "slack:T1:C1:100.1"
    assert mention.payload["text"] == "run &&"

    gateway = SlackSocketGateway(inbox, Clock(), normalizer)
    await gateway.handle_message(
        json.dumps(
            slack_envelope(
                "app_mention",
                {
                    "user": "U1",
                    "text": "<@UBOT> run &amp;&amp;",
                    "channel": "C1",
                    "ts": "100.1",
                },
            )
        ),
        _ignore_ack,
    )
    reply = await normalizer(
        slack_envelope(
            "message",
            {
                "user": "U2",
                "text": "continue",
                "channel": "C1",
                "channel_type": "channel",
                "thread_ts": "100.1",
                "ts": "101.1",
            },
            event_id="Ev2",
        )
    )
    assert reply is not None and reply.conversation_key == mention.conversation_key
    assert (
        await normalizer(
            slack_envelope(
                "message",
                {"user": "U1", "text": "DM", "channel": "D1", "channel_type": "im", "ts": "1.1"},
                event_id="Ev3",
            )
        )
        is not None
    )
    assert (
        await normalizer(
            slack_envelope(
                "message",
                {
                    "user": "U1",
                    "text": "top-level",
                    "channel": "C2",
                    "channel_type": "channel",
                    "ts": "1.2",
                },
                event_id="Ev4",
            )
        )
        is None
    )
    stop = await normalizer(
        slack_envelope(
            "message",
            {
                "user": "U2",
                "text": "停止",
                "channel": "C1",
                "channel_type": "channel",
                "thread_ts": "100.1",
                "ts": "102.1",
            },
            event_id="Ev5",
        )
    )
    assert stop is not None and stop.sender_id == "U2" and stop.kind == "stop_candidate"


@pytest.mark.asyncio
async def test_mention_and_message_delivery_order_deduplicates_by_message_receipt():
    async def participating(_key):
        return True

    normalizer = SlackEventNormalizer(bot_user_id="UBOT", is_participating=participating)
    for order in (("message", "app_mention"), ("app_mention", "message")):
        inbox = FakeInboxStore()
        if order[0] == "message":
            inbox.conversations["slack:T1:C1:100.1"] = {"participating": True}
        gateway = SlackSocketGateway(inbox, Clock(), normalizer)
        for event_type in order:
            await gateway.handle_message(
                json.dumps(
                    slack_envelope(
                        event_type,
                        {
                            "user": "U1",
                            "text": "<@UBOT> hello",
                            "channel": "C1",
                            "channel_type": "channel",
                            "thread_ts": "100.1",
                            "ts": "101.1",
                        },
                        event_id=f"Ev-{event_type}",
                    )
                ),
                _ignore_ack,
            )
        assert len(inbox.inputs) == 1
        assert sum(key[0] == "message" for key in inbox.receipts) == 1


async def _participates(inbox, key):
    return bool(inbox.conversations.get(key, {}).get("participating"))


async def _ignore_ack(_response):
    return None
