"""Ordering rules for closing an idle actor before releasing its Worker lease."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.distributed_ports import OwnershipStore
from silk_agent.core.ports import Clock


class OwnershipLifecycle:
    """Close an idle conversation actor before its durable ownership is released."""

    def __init__(self, ownership: OwnershipStore, clock: Clock) -> None:
        self._ownership = ownership
        self._clock = clock
        self._lock = asyncio.Lock()

    async def release_when_idle(
        self,
        token: OwnershipToken,
        *,
        is_idle: Callable[[], bool],
        save_and_close: Callable[[], Awaitable[None]],
    ) -> bool:
        """Save and close before transactionally resolving pending input and releasing.

        A failed close leaves the lease assigned to this actor, so another Worker cannot
        start an SDK session based on a release that did not finish cleanly.
        """
        async with self._lock:
            if not is_idle():
                return False
            await save_and_close()
            return await self._ownership.release(token, now=self._clock.now())
