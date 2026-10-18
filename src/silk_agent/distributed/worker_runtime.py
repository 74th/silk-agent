"""Process lifecycle coordinating Worker assignments and its liveness heartbeat."""

from __future__ import annotations

import asyncio
from typing import Protocol


class AssignmentWorker(Protocol):
    async def run(self, stop: asyncio.Event) -> None: ...


class Heartbeat(Protocol):
    async def register(self) -> None: ...

    async def run_registered(self, stop: asyncio.Event) -> None: ...


class Responder(Protocol):
    async def run(self, stop: asyncio.Event) -> None: ...


class WorkerProcessRuntime:
    """Register once, then keep heartbeat and assignment watch in one process lifetime."""

    def __init__(
        self,
        worker: AssignmentWorker,
        heartbeat: Heartbeat,
        responders: tuple[Responder, ...] = (),
    ) -> None:
        self._worker = worker
        self._heartbeat = heartbeat
        self._responders = responders

    async def run(self, stop: asyncio.Event) -> None:
        if stop.is_set():
            return
        await self._heartbeat.register()
        tasks = {
            asyncio.create_task(self._worker.run(stop), name="worker-assignments"),
            asyncio.create_task(self._heartbeat.run_registered(stop), name="worker-heartbeat"),
            *(
                asyncio.create_task(responder.run(stop), name="worker-responder")
                for responder in self._responders
            ),
        }
        try:
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            # Either an external stop or an unexpected component exit ends the process role.
            stop.set()
            # Propagate a failed component before awaiting a peer that might still be
            # blocked. The finally block cancels and joins every component.
            for task in done:
                task.result()
            await asyncio.gather(*tasks)
        finally:
            stop.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
