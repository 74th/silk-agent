from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.events import AssistantText
from silk_agent.distributed.outbox_sink import SanitizedOutboxSink


class Writer:
    def __init__(self):
        self.records = []

    async def put_sanitized(self, **record):
        self.records.append(record)
        return True


@pytest.mark.asyncio
async def test_outbox_adapter_persists_only_the_sanitized_event():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 4, 2, now + timedelta(minutes=1)
    )
    writer = Writer()

    async def owner_for(key):
        assert key == owner.conversation_key
        return owner

    async def next_sequence(_key):
        return 7

    class Clock:
        def now(self):
            return now

    sink = SanitizedOutboxSink(writer, Clock(), owner_for, next_sequence)
    await sink(AssistantText("conversation", "turn", "masked output"))
    record = writer.records[0]
    assert record["conversation_generation"] == 2
    assert record["sequence"] == 7
    assert record["payload"]["event"]["text"] == "masked output"
    assert record["payload"]["kind"] == "AssistantText"
