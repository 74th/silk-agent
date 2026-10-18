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
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from silk_agent.core.commands import CommandTracker
from silk_agent.core.ports import CommandResult, SandboxRuntime
from silk_agent.sandbox.monitor import CancelSender, CommandMonitor

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
    cancel_unconfirmed: bool = False  # the runtime could not confirm the stop: it may still run
    started: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(frozen=True)
class CommandCancelReport:
    requested: int = 0
    confirmed: int = 0
    accepted: int = 0
    unknown: int = 0
    rejected: int = 0
    too_late: int = 0


class BackgroundCommands:
    def __init__(
        self,
        runtime: SandboxRuntime,
        tracker: CommandTracker | None = None,
        *,
        command_monitor: CommandMonitor | None = None,
        cancel_sender: CancelSender | None = None,
    ) -> None:
        self._runtime = runtime
        self._tracker = tracker
        self._command_monitor = command_monitor
        self._cancel_sender = cancel_sender or getattr(runtime, "cancel_command", None)
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

    async def detach_durable(self, entry: BackgroundCommand) -> CommandResult | None:
        """Persist the foreground/background race before returning a tool response."""
        if self._command_monitor is None or not entry.command_id:
            return self.detach(entry)
        decision = await self._command_monitor.detach(entry.command_id)
        if decision is None:
            return CommandResult(entry.command_id, "unknown")
        if decision.get("decision") == "return_result":
            result = _result_from_record(entry.command_id, decision)
            entry.result = result
            return result
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

    async def cancel(
        self, key: str, command_id: str, *, reason: str = "command_tool"
    ) -> bool | str:
        entry = self.find(key, command_id)
        if self._command_monitor is not None:

            async def send_cancel(
                conversation_key: str, stable_command_id: str, request_id: str
            ) -> str:
                sender = self._cancel_sender
                if sender is None:
                    return "unknown"
                outcome = await sender(conversation_key, stable_command_id, request_id)
                return outcome if isinstance(outcome, str) else "unknown"

            decision = await self._command_monitor.cancel(
                command_id, reason=reason, send_cancel=send_cancel
            )
            if decision is None:
                return False
            outcome = str(decision.get("cancel_outcome", "unknown"))
            if entry is not None and outcome in {"accepted", "unknown", "confirmed"}:
                entry.cancelled = True
                if entry.task is not None and not entry.task.done():
                    entry.task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await entry.task
            return outcome
        if entry is None or entry.result is not None:
            return False
        outcome = await self._cancel(entry)
        return True if outcome == "confirmed" else outcome

    async def status(self, key: str, command_id: str) -> CommandResult:
        entry = self.find(key, command_id)
        if self._command_monitor is not None:
            record = await self._command_monitor.get_command(command_id)
            if record is not None:
                return _result_from_record(command_id, record)
            return CommandResult(command_id, "unknown")
        if entry is not None and entry.result is not None:
            return entry.result
        if entry is not None and entry.result is None:
            return CommandResult(command_id, "running")
        return await self._runtime.command_status(key, command_id)

    async def output(self, key: str, command_id: str, cursor: int) -> tuple[str, int]:
        if self._command_monitor is not None:
            archived = await self._command_monitor.command_output(command_id, cursor=cursor)
            if archived is not None:
                return archived
        return await self._runtime.command_output(key, command_id, cursor)

    async def cancel_all(self, key: str) -> CommandCancelReport:
        """Stop every running command of the conversation (no completion notices)."""
        entries = self.running(key)
        counts = {"confirmed": 0, "accepted": 0, "unknown": 0}
        for entry in entries:
            outcome = await self._cancel(entry)
            counts[outcome if outcome in counts else "unknown"] += 1
        return CommandCancelReport(requested=len(entries), **counts)

    async def orphan_all(self, key: str) -> int:
        """Leave remote commands running while detaching this actor's foreground waits."""
        entries = self.running(key)
        for entry in entries:
            if self._command_monitor is not None and entry.command_id:
                await self._command_monitor.orphan(entry.command_id)
            if entry.task is not None and not entry.task.done():
                entry.task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await entry.task
        return len(entries)

    async def cancel_all_for_stop(
        self, key: str, *, conversation_generation: int, stop_seq: int, page_size: int = 100
    ) -> CommandCancelReport:
        if page_size < 1:
            raise ValueError("page_size must be positive")
        if self._command_monitor is None:
            return await self.cancel_all(key)
        counts = {
            "requested": 0,
            "confirmed": 0,
            "accepted": 0,
            "unknown": 0,
            "rejected": 0,
            "too_late": 0,
        }
        cursor = None
        while True:
            records, cursor = await self._command_monitor.list_for_stop(
                conversation_key=key,
                conversation_generation=conversation_generation,
                stop_seq=stop_seq,
                limit=page_size,
                cursor=cursor,
            )
            for record in records:
                command_id = record.get("command_id")
                if not isinstance(command_id, str) or not command_id:
                    continue
                outcome = await self.cancel(key, command_id, reason="user_stop")
                if outcome is False:
                    continue
                counts["requested"] += 1
                category = outcome if isinstance(outcome, str) else "unknown"
                if category not in counts or category == "requested":
                    category = "unknown"
                counts[category] += 1
            if cursor is None:
                break
        return CommandCancelReport(**counts)

    async def _cancel(self, entry: BackgroundCommand) -> str:
        """Stop one command; "confirmed", or "accepted" / "unknown" when the stop is not proven.

        A runtime with `cancel_command` (a remote Sandbox) is asked to stop the process by its
        id: cancelling the asyncio wait alone leaves it running there. Without one, cancelling
        the wait stops the process (LocalProcess)."""
        entry.cancelled = True
        outcome = "confirmed"
        if self._cancel_sender is not None and entry.command_id and entry.result is None:
            try:
                sent = await self._cancel_sender(
                    entry.conversation_key, entry.command_id, uuid.uuid4().hex
                )
                outcome = sent if isinstance(sent, str) else "unknown"
            except Exception:
                log.exception("cancelling command %s failed", entry.command_id)
                outcome = "unknown"
        # Keep the id (and report "unknown") for a command that may still be running.
        entry.cancel_unconfirmed = outcome != "confirmed"
        if entry.task is not None and not entry.task.done():
            entry.task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await entry.task
        return outcome

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
            result = CommandResult(
                entry.command_id, "unknown" if entry.cancel_unconfirmed else "cancelled"
            )
            raise
        finally:
            entry.result = result or CommandResult(entry.command_id, "unknown")
            entry.started.set()
            # Keep the id only while the command may still be running (unknown / running).
            may_run = entry.result.outcome in ("unknown", "running")
            if self._tracker is not None and entry.command_id and not may_run:
                await self._tracker.finished(key, entry.command_id)
            self._forget(entry)
        if (
            entry.backgrounded
            and not entry.cancelled
            and self._command_monitor is None
            and self.on_finished is not None
        ):
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


def _result_from_record(command_id: str, record: dict | object) -> CommandResult:
    if not isinstance(record, dict):
        return CommandResult(command_id, "unknown")
    state = str(record.get("state", "unknown"))
    result = record.get("result")
    result_data = result if isinstance(result, dict) else {}
    if state in {"finished", "completed"}:
        return CommandResult(
            command_id,
            "completed",
            result_data.get("exit_code") if isinstance(result_data.get("exit_code"), int) else None,
            str(_stream_tail(result_data, "stdout")),
            str(_stream_tail(result_data, "stderr")),
        )
    if state == "cancelled":
        return CommandResult(command_id, "cancelled")
    if state in {"intent", "running", "cancel_requested"}:
        return CommandResult(command_id, "running")
    return CommandResult(command_id, "unknown")


def _stream_tail(result: dict, stream: str) -> str:
    output = result.get("output")
    if not isinstance(output, dict):
        return ""
    entry = output.get(stream)
    if not isinstance(entry, dict):
        return ""
    tail = entry.get("tail")
    return tail if isinstance(tail, str) else ""
