"""Command monitoring inside the Sandbox Service, independent of any Worker or API request.

The Service can read a command's state for a conversation whose Worker ownership has been
released; a Worker can not (its calls require a live owner). It never starts a Sandbox.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from silk_agent.core.ports import CommandResult
from silk_agent.sandbox.output_archive import OutputPage
from silk_agent.sandbox.service import _call_and_close, _session_key

log = logging.getLogger(__name__)


class ServiceCommandRuntime:
    """CommandStatusReader + OutputPageReader over the Sandbox manager (no ensure/start)."""

    def __init__(self, manager: Any) -> None:
        self._manager = manager

    async def _handle(self, conversation_key: str) -> Any | None:
        return await self._manager.get(_session_key(conversation_key))

    async def command_status(self, conversation_key: str, command_id: str) -> CommandResult:
        handle = await self._handle(conversation_key)
        if handle is None:
            return CommandResult(command_id, "unknown")
        try:
            record = await _call_and_close(handle.commands, "get", command_id)
        except Exception:
            return CommandResult(command_id, "unknown")
        state = record.get("state") if hasattr(record, "get") else None
        if state == "running":
            return CommandResult(command_id, "running")
        if state == "finished":
            outcome = "cancelled" if record.get("stop_reason") == "cancelled" else "completed"
            return CommandResult(command_id, outcome, record.get("exit_code"))
        return CommandResult(command_id, "unknown")

    async def read_output_page(
        self,
        conversation_key: str,
        command_id: str,
        *,
        stream: str,
        cursor: int,
        limit_bytes: int,
    ) -> OutputPage:
        handle = await self._handle(conversation_key)
        if handle is None:
            raise RuntimeError("Sandbox is not available for output read")
        page = await _call_and_close(
            handle.commands,
            "read_output",
            command_id,
            stream=stream,
            cursor=cursor,
            limit=limit_bytes,
        )
        return OutputPage(page.data, page.cursor, bool(page.complete and page.output_complete))


async def run_monitor_loop(
    monitor: Any,
    stop: asyncio.Event,
    *,
    poll_seconds: float = 2.0,
    on_tick: Callable[[], None] | None = None,
) -> None:
    """Poll due commands until stopped; any instance may take over another's lease."""
    while not stop.is_set():
        if on_tick is not None:
            on_tick()
        try:
            await monitor.poll_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("command monitor pass failed (%s)", type(exc).__name__)
        try:
            await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
        except TimeoutError:
            pass
