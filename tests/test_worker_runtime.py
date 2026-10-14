import asyncio

import pytest

from silk_agent.distributed.worker_runtime import WorkerProcessRuntime


class FakeWorker:
    def __init__(self):
        self.started = asyncio.Event()

    async def run(self, stop):
        self.started.set()
        await stop.wait()


class FakeHeartbeat:
    def __init__(self):
        self.registered = False
        self.started = asyncio.Event()

    async def register(self):
        self.registered = True

    async def run_registered(self, stop):
        assert self.registered
        self.started.set()
        await stop.wait()


@pytest.mark.asyncio
async def test_worker_process_registers_then_stops_both_components_together():
    worker = FakeWorker()
    heartbeat = FakeHeartbeat()
    stop = asyncio.Event()
    runtime = WorkerProcessRuntime(worker, heartbeat)

    task = asyncio.create_task(runtime.run(stop))
    await worker.started.wait()
    await heartbeat.started.wait()
    stop.set()
    await task

    assert heartbeat.registered
    assert task.done()


@pytest.mark.asyncio
async def test_failed_component_cancels_peer_even_when_peer_ignores_stop_event():
    class BlockedWorker:
        def __init__(self):
            self.cancelled = asyncio.Event()

        async def run(self, stop):
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled.set()

    class FailingHeartbeat(FakeHeartbeat):
        async def run_registered(self, stop):
            await asyncio.sleep(0)
            raise RuntimeError("heartbeat identity rejected")

    worker = BlockedWorker()
    with pytest.raises(RuntimeError, match="rejected"):
        await asyncio.wait_for(
            WorkerProcessRuntime(worker, FailingHeartbeat()).run(asyncio.Event()), timeout=1
        )
    assert worker.cancelled.is_set()
