"""SlackChannel: Bolt (AsyncApp + Socket Mode) adapter.

Inbound: Slack events -> `SlackEventRouter` -> orchestrator.
Outbound: core events -> progress message / replies (see `slack_output.SlackOutput`).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from silk_agent.channels.slack_events import SlackEventRouter
from silk_agent.config import ProgressConfig
from silk_agent.core.events import InboundEvent, OutboundEvent
from silk_agent.core.ports import Clock, SessionStateStore

log = logging.getLogger(__name__)


class SlackChannel:
    def __init__(
        self,
        app: Any,
        handle: Callable[[InboundEvent], Awaitable[None]],
        clock: Clock,
        progress: ProgressConfig,
        *,
        state_store: SessionStateStore | None = None,
        bot_user_id: str = "",
    ) -> None:
        from silk_agent.channels.slack_output import SlackOutput

        self._app = app
        self._state_store = state_store
        self._participating: set[str] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self.router = SlackEventRouter(
            handle,
            bot_user_id=bot_user_id,
            is_participating=self._is_participating,
            mark_participating=self._participating.add,
        )
        self.output = SlackOutput(app.client, clock, progress, state_store=state_store)

    # ------------------------------------------------------------ inbound

    async def _is_participating(self, key: str) -> bool:
        """The Bot took part in a thread if it started it, or if a state record exists
        (so threads stay alive across Bot restarts)."""
        if key in self._participating:
            return True
        if self._state_store is not None:
            try:
                if await self._state_store.get(key) is not None:
                    self._participating.add(key)
                    return True
            except Exception:
                log.exception("participation lookup failed for %s", key)
        return False

    def register(self) -> None:
        """Attach the Bolt listeners. Listeners return immediately (Bolt acks within 3s);
        the work runs in a separate task."""

        @self._app.event("app_mention")
        async def on_mention(body: dict[str, Any]) -> None:
            self._spawn(self.router.on_app_mention(body))

        @self._app.event("message")
        async def on_message(body: dict[str, Any]) -> None:
            self._spawn(self.router.on_message(body))

    def _spawn(self, coro: Awaitable[None]) -> None:
        async def run() -> None:
            try:
                await coro
            except Exception:
                log.exception("slack event handler failed")

        task = asyncio.create_task(run())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def wait_idle(self) -> None:
        """Wait for in-flight event handlers (used by tests and graceful shutdown)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # ------------------------------------------------------------ outbound

    async def send(self, event: OutboundEvent) -> None:
        await self.output.send(event)
