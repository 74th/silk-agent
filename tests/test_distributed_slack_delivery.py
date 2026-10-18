import asyncio
from datetime import UTC, datetime

import pytest

from silk_agent.channels.slack_output import SlackOutput
from silk_agent.config import ProgressConfig
from silk_agent.core.events import AssistantText, StatusChanged, TurnStarted
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.session import SessionSettings
from silk_agent.core.stopwords import StopWords
from silk_agent.distributed.outbox_sink import _json_value
from silk_agent.distributed.slack_delivery import SlackOutboxDelivery
from silk_agent.testing.fakes import AllowAllGuard, FakeSanitizer


class Clock:
    def now(self):
        return datetime(2026, 10, 14, tzinfo=UTC)

    async def sleep(self, _seconds):
        return None


class Client:
    def __init__(self):
        self.posts = []
        self.updates = []
        self.uploads = []

    async def chat_postMessage(self, **kwargs):
        self.posts.append(kwargs)
        return {"ts": str(len(self.posts))}

    async def chat_update(self, **kwargs):
        self.updates.append(kwargs)
        return {}

    async def files_upload_v2(self, **kwargs):
        self.uploads.append(kwargs)
        return {}


@pytest.mark.asyncio
async def test_sanitized_outbox_delivery_uses_slack_progress_and_long_reply_upload():
    client = Client()
    delivery = SlackOutboxDelivery(SlackOutput(client, Clock(), ProgressConfig()))
    key = "slack:T1:C1:100.1"

    async def deliver(event):
        await delivery({"payload": {"kind": type(event).__name__, "event": _json_value(event)}})

    await deliver(TurnStarted(key, "turn-1", Clock().now()))
    await deliver(StatusChanged(key, "turn-1", "preparing_sandbox"))
    await asyncio.sleep(0)
    assert client.posts[0]["text"].startswith("⏳ 実行中")
    assert client.updates
    assert "作業環境を起動" in client.updates[-1]["text"]

    await deliver(AssistantText(key, "turn-1", "x" * 12_001))
    assert client.uploads[0]["thread_ts"] == "100.1"
    assert len(client.uploads[0]["content"]) == 12_001


@pytest.mark.asyncio
async def test_outbox_delivery_rejects_unknown_event_kind():
    delivery = SlackOutboxDelivery(SlackOutput(Client(), Clock(), ProgressConfig()))
    with pytest.raises(ValueError, match="unsupported"):
        await delivery({"payload": {"kind": "RawSdkEvent", "event": {}}})


@pytest.mark.asyncio
async def test_long_reply_is_sanitized_before_being_uploaded_from_outbox():
    client = Client()
    delivery = SlackOutboxDelivery(SlackOutput(client, Clock(), ProgressConfig()))

    async def sink(event):
        await delivery({"payload": {"kind": type(event).__name__, "event": _json_value(event)}})

    class Channel:
        async def send(self, _event):
            raise AssertionError("distributed delivery should use the Outbox")

    orchestrator = ConversationOrchestrator(
        lambda _key: None,
        Channel(),
        Clock(),
        guard=AllowAllGuard(),
        sanitizer=FakeSanitizer(["secret"]),
        stop_words=StopWords([]),
        settings=SessionSettings(),
        outbox_sink=sink,
    )
    await orchestrator.emit(AssistantText("slack:T1:C1:100.1", "turn-1", "secret " + "x" * 12_001))

    assert len(client.uploads) == 1
    assert "secret" not in client.uploads[0]["content"]
    assert client.uploads[0]["content"].startswith("[MASKED]")
