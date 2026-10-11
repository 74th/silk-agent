"""BackgroundCommands: sandbox commands that keep running after the tool call returned.

The `bash` tool starts every command here and waits only a short time. A command that
is still running is left to its asyncio task, and the agent gets its command_id right
away, so the conversation can go on (new instructions, other tasks). When a backgrounded
command finishes, `on_finished(conversation_key, notice)` tells the conversation.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from silk_agent.core.commands import CommandTracker
from silk_agent.core.ports import CommandResult, SandboxRuntime

log = logging.getLogger(__name__)

NOTICE_OUTPUT_CHARS = 3000

FinishedCallback = Callable[[str, str], Awaitable[None]]


@dataclass
class BackgroundCommand:
    conversation_key: str
    command: str
    command_id: str = ""
    task: asyncio.Task[CommandResult] | None = None
    result: CommandResult | None = None
    backgrounded: bool = False  # the tool call returned before the command finished
    cancelled: bool = False  # stopped on request: no completion notice
    started: asyncio.Event = field(default_factory=asyncio.Event)


class BackgroundCommands:
    def __init__(self, runtime: SandboxRuntime, tracker: CommandTracker | None = None) -> None:
        self._runtime = runtime
        self._tracker = tracker
        self._commands: dict[str, list[BackgroundCommand]] = {}
        self.on_finished: FinishedCallback | None = None

    # ------------------------------------------------------------ start / wait

    async def start(
        self,
        key: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout_seconds: float | None = None,
    ) -> BackgroundCommand:
        """Submit the command; returns once it has an id (or has already ended)."""
        entry = BackgroundCommand(key, command)

        async def on_started(command_id: str) -> None:
            entry.command_id = command_id
            if self._tracker is not None:
                await self._tracker.started(key, command_id)
            entry.started.set()

        entry.task = asyncio.create_task(self._run(entry, on_started, cwd, timeout_seconds))
        self._commands.setdefault(key, []).append(entry)
        started = asyncio.ensure_future(entry.started.wait())
        try:
            await asyncio.wait({started, entry.task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            started.cancel()
        if entry.task.done() and entry.task.exception() is not None:
            self._forget(entry)
            raise entry.task.exception()  # type: ignore[misc]
        return entry

    async def wait(self, entry: BackgroundCommand, seconds: float) -> CommandResult | None:
        """The result if the command ends within `seconds`; None if it is still running.
        Cancelling the caller (e.g. an interrupted turn) does not cancel the command."""
        assert entry.task is not None
        try:
            return await asyncio.wait_for(asyncio.shield(entry.task), seconds)
        except TimeoutError:
            return None

    def detach(self, entry: BackgroundCommand) -> CommandResult | None:
        """Leave the command running in the background (a completion notice will follow).
        Returns the result instead when it finished in the meantime."""
        if entry.result is not None:
            return entry.result
        entry.backgrounded = True
        return None

    # ------------------------------------------------------------ queries / cancel

    def running(self, key: str) -> list[BackgroundCommand]:
        return [e for e in self._commands.get(key, []) if e.result is None]

    def find(self, key: str, command_id: str) -> BackgroundCommand | None:
        for entry in self._commands.get(key, []):
            if entry.command_id == command_id:
                return entry
        return None

    async def cancel(self, key: str, command_id: str) -> bool:
        entry = self.find(key, command_id)
        if entry is None or entry.result is not None:
            return False
        await self._cancel(entry)
        return True

    async def cancel_all(self, key: str) -> int:
        """Stop every running command of the conversation (no completion notices)."""
        entries = self.running(key)
        for entry in entries:
            await self._cancel(entry)
        return len(entries)

    async def _cancel(self, entry: BackgroundCommand) -> None:
        entry.cancelled = True
        if entry.task is not None and not entry.task.done():
            entry.task.cancel()  # the runtime stops the process when its wait is cancelled
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await entry.task

    # ------------------------------------------------------------ internals

    async def _run(
        self,
        entry: BackgroundCommand,
        on_started: Callable[[str], Awaitable[None]],
        cwd: str | None,
        timeout_seconds: float | None,
    ) -> CommandResult:
        key = entry.conversation_key
        result: CommandResult | None = None
        try:
            result = await self._runtime.run_command(
                key,
                entry.command,
                cwd=cwd,
                timeout_seconds=timeout_seconds,
                on_started=on_started,
            )
        except asyncio.CancelledError:
            result = CommandResult(entry.command_id, "cancelled")
            raise
        finally:
            entry.result = result or CommandResult(entry.command_id, "unknown")
            entry.started.set()
            # Keep the id only while the command may still be running (unknown / running).
            may_run = entry.result.outcome in ("unknown", "running")
            if self._tracker is not None and entry.command_id and not may_run:
                await self._tracker.finished(key, entry.command_id)
            self._forget(entry)
        if entry.backgrounded and not entry.cancelled and self.on_finished is not None:
            try:
                await self.on_finished(key, finished_notice(entry))
            except Exception:
                log.exception("could not deliver the completion of %s", entry.command_id)
        return entry.result

    def _forget(self, entry: BackgroundCommand) -> None:
        entries = self._commands.get(entry.conversation_key, [])
        if entry in entries:
            entries.remove(entry)


def _tail(text: str, limit: int = NOTICE_OUTPUT_CHARS) -> str:
    if len(text) <= limit:
        return text
    return f"…（先頭 {len(text) - limit} 文字を省略）\n" + text[-limit:]


def finished_notice(entry: BackgroundCommand) -> str:
    result = entry.result
    assert result is not None
    head = (
        f"バックグラウンドで実行していたコマンドが終了しました"
        f"（command_id={result.command_id or entry.command_id}）。\n"
        f"コマンド: {entry.command}\n"
    )
    if result.outcome == "completed":
        body = f"exit code: {result.exit_code}\n"
        if result.stdout:
            body += "[stdout]\n" + _tail(result.stdout) + "\n"
        if result.stderr:
            body += "[stderr]\n" + _tail(result.stderr) + "\n"
    elif result.outcome == "cancelled":
        body = "コマンドは取り消されました（タイムアウトを含む）。\n"
    else:
        body = (
            "結果を確認できませんでした。同じコマンドを再実行せず、"
            "command_status と command_output で確認してください。\n"
        )
    return (
        f"<system-notice>{head}{body}</system-notice>\n"
        "この結果をユーザに簡潔に報告してください。続きの作業があれば進めてください。"
    )
