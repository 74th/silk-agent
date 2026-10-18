from __future__ import annotations

import asyncio
from collections.abc import Callable
from threading import Thread

from silk_agent.stores.firestore.watch import WatchErrorCallback, WatchHandle


class FakeWatchHandle:
    def __init__(self) -> None:
        self.unsubscribed = False

    def unsubscribe(self) -> None:
        self.unsubscribed = True


class FakeWatchSource:
    def __init__(self) -> None:
        self.callbacks: list[tuple[Callable[[], None], WatchErrorCallback, FakeWatchHandle]] = []
        self.subscribed = asyncio.Event()

    def subscribe(self, on_change: Callable[[], None], on_error: WatchErrorCallback) -> WatchHandle:
        handle = FakeWatchHandle()
        self.callbacks.append((on_change, on_error, handle))
        self.subscribed.set()
        return handle

    async def wait_for_subscriptions(self, count: int) -> None:
        while len(self.callbacks) < count:
            self.subscribed.clear()
            await self.subscribed.wait()

    async def emit_from_thread(self, index: int = -1) -> None:
        callback = self.callbacks[index][0]

        def invoke() -> None:
            thread = Thread(target=callback)
            thread.start()
            thread.join()

        await asyncio.to_thread(invoke)

    async def fail_from_thread(self, error: BaseException, index: int = -1) -> None:
        callback = self.callbacks[index][1]

        def invoke() -> None:
            thread = Thread(target=lambda: callback(error))
            thread.start()
            thread.join()

        await asyncio.to_thread(invoke)
