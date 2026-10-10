"""Persists the ids of sandbox commands that are running, per conversation."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict

from silk_agent.core.ports import ConversationState, SessionStateStore

log = logging.getLogger(__name__)


class CommandTracker:
    """`started` / `finished` keep `ConversationState.running_command_ids` up to date so a
    resumed conversation can tell the agent which commands were running when the Bot died."""

    def __init__(self, store: SessionStateStore) -> None:
        self._store = store
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def started(self, key: str, command_id: str) -> None:
        await self._update(key, lambda ids: ids if command_id in ids else [*ids, command_id])

    async def finished(self, key: str, command_id: str) -> None:
        await self._update(key, lambda ids: [i for i in ids if i != command_id])

    async def _update(self, key: str, change) -> None:  # type: ignore[no-untyped-def]
        async with self._locks[key]:
            try:
                state = await self._store.get(key) or ConversationState(key)
                state.running_command_ids = change(list(state.running_command_ids))
                await self._store.put(state)
            except Exception:
                log.exception("could not persist command ids for %s", key)
