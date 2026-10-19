"""Command monitoring inside the Sandbox Service, independent of any Worker or API request.

The Service can read a command's state for a conversation whose Worker ownership has been
released; a Worker can not (its calls require a live owner). Reading a command's state never
starts a Sandbox (`observe_commands` only looks at one that exists).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from gke_session_sandbox.errors import (
    SandboxLostError,
    SandboxNotReadyError,
    StaleGenerationError,
)
from silk_agent.core.ports import CommandResult
from silk_agent.distributed.metrics import METRICS
from silk_agent.sandbox.output_archive import OutputPage
from silk_agent.sandbox.service import _call_and_close, _session_key

log = logging.getLogger(__name__)


class ServiceCommandRuntime:
    """CommandStatusReader + OutputPageReader over the Sandbox manager (no ensure/start).

    It reads through `observe_commands`, which never resumes a suspended Sandbox or creates a
    new one."""

    # `CommandMonitor` passes the generation the command ran in, so a Sandbox that was
    # replaced (or lost) ends the command instead of being read as "unknown" forever.
    reads_by_generation = True

    def __init__(self, manager: Any) -> None:
        self._manager = manager

    async def _handle(self, conversation_key: str) -> Any | None:
        return await self._manager.get(_session_key(conversation_key))

    async def command_status(
        self, conversation_key: str, command_id: str, *, sandbox_generation: int | None = None
    ) -> CommandResult:
        handle = await self._handle(conversation_key)
        if handle is None:
            return CommandResult(command_id, "lost")
        if sandbox_generation is not None and handle.generation != sandbox_generation:
            # The command ran in an earlier generation of the Sandbox, which is gone.
            return CommandResult(command_id, "lost")
        try:
            record = await _call_and_close(handle.observe_commands, "get", command_id)
        except (SandboxLostError, StaleGenerationError):
            return CommandResult(command_id, "lost")
        except Exception as exc:
            # Not ready (suspended) or unreachable: nothing is known, and nothing is started.
            # Only a fixed class is logged: the text of the exception can carry the body of the
            # tracker's response.
            reason = (
                "sandbox_not_ready"
                if isinstance(exc, SandboxNotReadyError)
                else f"error:{type(exc).__name__}"
            )
            log.warning("command status of %s unavailable (%s)", command_id, reason)
            return CommandResult(command_id, "unknown")
        state = record.get("state") if hasattr(record, "get") else None
        if state == "running":
            return CommandResult(command_id, "running")
        if state == "finished":
            _note_output_limit(command_id, record)
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
            handle.observe_commands,
            "read_output",
            command_id,
            stream=stream,
            cursor=cursor,
            limit=limit_bytes,
        )
        complete = bool(page.complete)
        return OutputPage(page.data, page.cursor, complete, complete and not page.output_complete)


_OUTPUT_LIMITS = {"session_output_limit": "session", "command_output_limit": "command"}


def _note_output_limit(command_id: str, record: Any) -> None:
    """Make a command that ran into an output limit visible: its output is cut short and, for the
    session limit, the next commands of the session fail the same way until old output expires."""
    limit = _OUTPUT_LIMITS.get(str(record.get("output_error_code")))
    if limit is None:
        return
    METRICS.inc("sandbox_output_limit_total", limit=limit)
    log.warning("command %s hit the %s output limit; its output is incomplete", command_id, limit)


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
