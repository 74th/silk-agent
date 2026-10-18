import asyncio
from dataclasses import dataclass

import pytest

from silk_agent.core.distributed_models import InputDeliveryState
from silk_agent.stores.firestore.watch import FirestoreWatchAdapter
from tests.fakes.distributed_runtime import FakeClock
from tests.fakes.distributed_watch import FakeWatchSource


@dataclass(frozen=True)
class PendingItem:
    input_id: str
    state: InputDeliveryState


@pytest.mark.asyncio
async def test_watch_bridge_coalesces_duplicate_signals_and_unsubscribes_on_cancel():
    source = FakeWatchSource()
    pending = [PendingItem("input-1", InputDeliveryState.PENDING)]
    reads = 0
    consumed: list[PendingItem] = []
    consumed_once = asyncio.Event()
    duplicate_signal_read = asyncio.Event()

    async def read_pending():
        nonlocal reads
        reads += 1
        if reads >= 2:
            duplicate_signal_read.set()
        return tuple(pending)

    async def consume(item: PendingItem) -> None:
        consumed.append(item)
        consumed_once.set()

    adapter = FirestoreWatchAdapter(
        subscribe=source.subscribe,
        read_pending=read_pending,
        consume=consume,
        item_key=lambda item: item.input_id,
        item_version=lambda item: item.state,
    )
    task = asyncio.create_task(adapter.run())
    try:
        await consumed_once.wait()
        await source.emit_from_thread()
        await duplicate_signal_read.wait()
        assert consumed == [PendingItem("input-1", InputDeliveryState.PENDING)]
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert source.callbacks[0][2].unsubscribed


@pytest.mark.asyncio
async def test_watch_reconnect_rereads_durable_pending_inputs():
    source = FakeWatchSource()
    clock = FakeClock()
    pending = [PendingItem("input-1", InputDeliveryState.PENDING)]
    consumed: list[str] = []
    first_consumed = asyncio.Event()
    recovered = asyncio.Event()

    async def read_pending():
        return tuple(pending)

    async def consume(item: PendingItem) -> None:
        consumed.append(item.input_id)
        if item.input_id == "input-1":
            first_consumed.set()
        if item.input_id == "input-2":
            recovered.set()

    adapter = FirestoreWatchAdapter(
        subscribe=source.subscribe,
        read_pending=read_pending,
        consume=consume,
        item_key=lambda item: item.input_id,
        item_version=lambda item: item.state,
        retry_seconds=0,
        clock=clock,
    )
    task = asyncio.create_task(adapter.run())
    try:
        await first_consumed.wait()
        pending.append(PendingItem("input-2", InputDeliveryState.PENDING))
        await source.fail_from_thread(ConnectionError("temporary watch failure"))
        await source.wait_for_subscriptions(2)
        await recovered.wait()
        assert consumed == ["input-1", "input-2"]
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert all(handle.unsubscribed for _, _, handle in source.callbacks)
