from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.core.distributed_models import OutputDeliveryState
from silk_agent.distributed.sender import OutboxSender


class Clock:
    def __init__(self):
        self.current = datetime(2026, 10, 14, tzinfo=UTC)

    def now(self):
        return self.current


class Outbox:
    def __init__(self):
        self.record = {
            "output_id": "output-1",
            "attempt_id": "attempt-1",
            "payload": {"text": "safe"},
        }
        self.marked = []
        self.claimed = []
        self.valid = True

    async def claim_next(self, **kwargs):
        self.claimed.append(kwargs)
        if self.record is None:
            return None
        record, self.record = self.record, None
        return record

    async def mark_delivery(self, **kwargs):
        self.marked.append(kwargs)
        return True

    async def revalidate_claim(self, **_kwargs):
        return self.valid


@pytest.mark.asyncio
async def test_sender_records_slack_reference_after_delivery():
    outbox = Outbox()
    sent = []

    async def deliver(record):
        sent.append(record["payload"])
        return {"channel": "C1", "ts": "123.4"}

    sender = OutboxSender(outbox, Clock(), deliver, sender_id="sender-a")
    assert await sender.run_once("conversation")
    assert sent == [{"text": "safe"}]
    assert outbox.marked[0]["new_state"] is OutputDeliveryState.DELIVERED
    assert outbox.marked[0]["slack_ref"] == {"channel": "C1", "ts": "123.4"}
    assert outbox.claimed[0]["lease_expires_at"] == Clock().current + timedelta(seconds=30)


@pytest.mark.asyncio
async def test_sender_marks_unknown_post_result_uncertain_without_retry():
    outbox = Outbox()
    attempts = 0

    async def disconnect_after_accept(_record):
        nonlocal attempts
        attempts += 1
        raise OSError("Slack response was lost")

    sender = OutboxSender(outbox, Clock(), disconnect_after_accept, sender_id="sender-a")
    assert await sender.run_once("conversation")
    assert attempts == 1
    assert outbox.marked[0]["new_state"] is OutputDeliveryState.UNCERTAIN


@pytest.mark.asyncio
async def test_sender_retries_rate_limit_after_retry_after():
    outbox = Outbox()

    class SlackResponse:
        status_code = 429
        headers = {"Retry-After": "12"}

    class RateLimitError(Exception):
        response = SlackResponse()

    async def limited(_record):
        raise RateLimitError()

    clock = Clock()
    sender = OutboxSender(outbox, clock, limited, sender_id="sender-a")
    assert await sender.run_once("conversation")
    update = outbox.marked[0]
    assert update["new_state"] is OutputDeliveryState.PENDING
    assert update["next_attempt_at"] == clock.current + timedelta(seconds=12)


@pytest.mark.asyncio
async def test_sender_does_not_send_after_generation_revalidation_fails():
    outbox = Outbox()
    outbox.valid = False
    sent = []

    async def deliver(record):
        sent.append(record)
        return None

    assert await OutboxSender(outbox, Clock(), deliver, sender_id="sender-a").run_once(
        "conversation"
    )
    assert sent == []
