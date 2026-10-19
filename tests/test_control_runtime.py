import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from silk_agent.distributed.control_runtime import ControlAllocatorLoop


class FakeClock:
    def now(self):
        return datetime(2026, 10, 14, tzinfo=UTC)


class FakeConversations:
    def __init__(self, waiting):
        self.waiting = waiting
        self.limit = None

    async def list_waiting(self, *, limit):
        self.limit = limit
        return self.waiting


class FakeWorkers:
    def __init__(self, candidates):
        self.candidates = candidates
        self.now = None

    async def list_available(self, *, now):
        self.now = now
        return self.candidates


class FakeAllocator:
    def __init__(self):
        self.calls = []

    async def allocate(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace() if kwargs["conversation_key"] == "room-1" else None


@pytest.mark.asyncio
async def test_control_allocator_assigns_each_waiting_conversation_with_live_candidates():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    conversations = FakeConversations(("room-1", "room-2"))
    workers = FakeWorkers(("worker-a",))
    allocator = FakeAllocator()
    runtime = ControlAllocatorLoop(
        conversations,
        workers,
        allocator,
        FakeClock(),
        allocator_id="control-a",
        lease_seconds=60,
        worker_lease_seconds=60,
        batch_size=25,
    )

    assigned = await runtime.run_once()

    assert assigned == 1
    assert conversations.limit == 25
    assert workers.now == now
    assert [call["conversation_key"] for call in allocator.calls] == ["room-1", "room-2"]
    assert allocator.calls[0]["allocator_lease_expires_at"] > now


@pytest.mark.asyncio
async def test_control_allocator_counts_waiting_conversations_but_assigns_none_without_workers():
    conversations = FakeConversations(("room-1",))
    allocator = FakeAllocator()
    runtime = ControlAllocatorLoop(
        conversations,
        FakeWorkers(()),
        allocator,
        FakeClock(),
        allocator_id="control-a",
    )

    assert await runtime.run_once() == 0
    # The queue is still read so its depth stays observable while no Worker is available.
    assert conversations.limit == 100
    assert allocator.calls == []


@pytest.mark.asyncio
async def test_sender_loop_drains_each_conversation_in_order_and_isolates_failures():
    from silk_agent.distributed.control_runtime import OutboxSenderLoop

    class Outbox:
        async def list_conversations_with_pending(self, *, limit):
            return ("room-1", "room-2", "room-3")

    class Sender:
        def __init__(self):
            self.remaining = {"room-1": 3, "room-2": 1, "room-3": 1}
            self.calls = []

        async def run_once(self, key):
            self.calls.append(key)
            if key == "room-2":
                raise ConnectionError("slack down")
            if self.remaining[key] == 0:
                return False
            self.remaining[key] -= 1
            return True

    sender = Sender()
    loop = OutboxSenderLoop(Outbox(), sender)
    assert await loop.run_once() == 4  # room-1 x3 + room-3 x1; room-2's error stays local
    assert sender.calls.count("room-1") == 4 and sender.calls.count("room-2") == 1


@pytest.mark.asyncio
async def test_a_conversation_that_always_has_output_does_not_hold_back_the_next_page():
    from silk_agent.distributed.control_runtime import OutboxSenderLoop

    pages = {None: (("busy",), "p1"), "p1": (("calm",), None)}
    sent = []

    class Outbox:
        async def list_pending_page(self, *, limit, after):
            return pages[after]

    class Sender:
        async def run_once(self, key):
            sent.append(key)
            return key == "busy"  # never runs dry

    loop = OutboxSenderLoop(Outbox(), Sender(), max_per_conversation=5)
    await asyncio.wait_for(loop.run_once(), 1)  # returns although "busy" never empties
    assert sent.count("busy") == 5 and loop.backlog
    await asyncio.wait_for(loop.run_once(), 1)
    assert "calm" in sent  # the next page was reached


@pytest.mark.asyncio
async def test_a_failing_listing_after_a_busy_pass_waits_for_the_poll_interval():
    from silk_agent.distributed.control_runtime import OutboxSenderLoop

    listings = []

    class Outbox:
        async def list_pending_page(self, *, limit, after):
            listings.append(1)
            if len(listings) == 1:
                return ("busy",), None
            raise ConnectionError("store down")

    class Sender:
        async def run_once(self, key):
            return True  # always has output: the first pass ends with a backlog

    stop = asyncio.Event()
    loop = OutboxSenderLoop(Outbox(), Sender(), poll_seconds=0.05, max_per_conversation=3)
    task = asyncio.create_task(loop.run(stop))
    await asyncio.sleep(0.2)
    stop.set()
    await asyncio.wait_for(task, 1)
    # Without the wait, the failing listing is retried thousands of times in 0.2 s.
    assert len(listings) <= 10


@pytest.mark.asyncio
async def test_sender_loop_resumes_after_the_last_page_and_starts_over_at_the_end():
    from silk_agent.distributed.control_runtime import OutboxSenderLoop

    pages = {None: (("stuck",), "p1"), "p1": (("stuck", "healthy"), "p2"), "p2": (("later",), None)}
    asked = []

    class Outbox:
        async def list_pending_page(self, *, limit, after):
            asked.append(after)
            return pages[after]

    class Sender:
        async def run_once(self, key):
            return False

    loop = OutboxSenderLoop(Outbox(), Sender())
    for _ in range(4):
        await loop.run_once()
    assert asked == [None, "p1", "p2", None]


@pytest.mark.asyncio
async def test_control_allocator_reports_the_oldest_wait_and_whether_the_count_is_capped():
    from datetime import timedelta

    from silk_agent.distributed.metrics import METRICS

    METRICS.reset()

    class Conversations(FakeConversations):
        async def oldest_waiting_since(self):
            return datetime(2026, 10, 14, tzinfo=UTC) - timedelta(seconds=90)

    runtime = ControlAllocatorLoop(
        Conversations(("room-1", "room-2")),
        FakeWorkers(()),
        FakeAllocator(),
        FakeClock(),
        allocator_id="control-a",
        batch_size=2,
    )
    await runtime.run_once()
    assert METRICS.value("control_oldest_waiting_seconds") == 90
    assert METRICS.value("control_waiting_conversations_capped") == 1  # the page is full
    METRICS.reset()
