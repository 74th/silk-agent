"""Thread-safe Firestore watch bridge with re-query and duplicate-signal coalescing."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol, TypeVar

from silk_agent.core.clock import SystemClock
from silk_agent.core.ports import Clock

T = TypeVar("T")


class WatchHandle(Protocol):
    def unsubscribe(self) -> None: ...


WatchCallback = Callable[[], None]
WatchErrorCallback = Callable[[BaseException], None]
WatchFactory = Callable[[WatchCallback, WatchErrorCallback], WatchHandle]
ReadPending = Callable[[], Awaitable[Sequence[T]]]
ConsumePending = Callable[[T], Awaitable[None]]
ItemKey = Callable[[T], str]
ItemVersion = Callable[[T], object]


class SnapshotQuery(Protocol):
    def on_snapshot(self, callback: Callable[..., None]) -> WatchHandle: ...


def firestore_watch_factory(query: SnapshotQuery) -> WatchFactory:
    """Adapt the sync Python client's background-thread snapshot callback."""

    def subscribe(on_change: WatchCallback, _on_error: WatchErrorCallback) -> WatchHandle:
        return query.on_snapshot(lambda *_args: on_change())

    return subscribe


class FirestoreWatchAdapter[T]:
    """Re-query durable work on watch signals and after every reconnection.

    Snapshot callbacks only wake the asyncio task. A set Event coalesces bursts while the
    consumer reads the canonical pending records from Firestore. Errors unsubscribe and
    reconnect; cancellation always closes the watch.
    """

    def __init__(
        self,
        *,
        subscribe: WatchFactory,
        read_pending: ReadPending[T],
        consume: ConsumePending[T],
        item_key: ItemKey[T],
        item_version: ItemVersion[T],
        retry_seconds: float = 1.0,
        clock: Clock | None = None,
    ) -> None:
        if retry_seconds < 0:
            raise ValueError("retry_seconds must be non-negative")
        self._subscribe = subscribe
        self._read_pending = read_pending
        self._consume = consume
        self._item_key = item_key
        self._item_version = item_version
        self._retry_seconds = retry_seconds
        self._clock = clock or SystemClock()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        changed = asyncio.Event()
        errors: asyncio.Queue[BaseException] = asyncio.Queue()
        last_delivered: dict[str, object] = {}

        def signal_change() -> None:
            loop.call_soon_threadsafe(changed.set)

        def signal_error(error: BaseException) -> None:
            loop.call_soon_threadsafe(errors.put_nowait, error)

        while True:
            handle: WatchHandle | None = None
            try:
                handle = self._subscribe(signal_change, signal_error)
                changed.set()  # Read durable state on startup and after reconnect.
                while True:
                    await _wait_for_signal(changed, errors)
                    records = await self._read_pending()
                    current: dict[str, object] = {}
                    for record in records:
                        key = self._item_key(record)
                        version = self._item_version(record)
                        current[key] = version
                        if last_delivered.get(key) == version:
                            continue
                        await self._consume(record)
                        last_delivered[key] = version
                    # Drop finished/removed inputs so a later legitimate re-queue is visible.
                    last_delivered = {
                        key: version
                        for key, version in last_delivered.items()
                        if current.get(key) == version
                    }
            except asyncio.CancelledError:
                raise
            except Exception:
                # Exception text may contain document paths or request data; do not log it.
                await self._clock.sleep(self._retry_seconds)
            finally:
                if handle is not None:
                    handle.unsubscribe()


async def _wait_for_signal(changed: asyncio.Event, errors: asyncio.Queue[BaseException]) -> None:
    change_task = asyncio.create_task(changed.wait())
    error_task = asyncio.create_task(errors.get())
    done, pending = await asyncio.wait(
        {change_task, error_task}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    if error_task in done:
        raise error_task.result()
    changed.clear()
