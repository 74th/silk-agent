"""In-process fakes for unit tests and local development (no external services)."""

from __future__ import annotations

import asyncio
import heapq
import itertools
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from silk_agent.core.events import (
    AssistantText,
    OutboundEvent,
    ToolFinished,
    ToolStarted,
    TurnFinished,
    Usage,
)
from silk_agent.core.ports import (
    CommandResult,
    ConversationState,
    FileEntry,
    GuardVerdict,
    SanitizeResult,
    TurnRequest,
)

# ---------------------------------------------------------------- Clock


class FakeClock:
    """Clock whose time only moves when the test calls advance()."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 10, 10, 9, 0, 0, tzinfo=UTC)
        self._sleepers: list[tuple[datetime, int, asyncio.Future[None]]] = []
        self._counter = itertools.count()

    def now(self) -> datetime:
        return self._now

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(
            self._sleepers, (self._now + timedelta(seconds=seconds), next(self._counter), fut)
        )
        await fut

    async def _settle(self) -> None:
        for _ in range(20):
            await asyncio.sleep(0)

    async def advance(self, seconds: float) -> None:
        """Move time forward, waking sleepers in deadline order."""
        target = self._now + timedelta(seconds=seconds)
        await self._settle()
        while self._sleepers and self._sleepers[0][0] <= target:
            deadline, _, fut = heapq.heappop(self._sleepers)
            self._now = max(self._now, deadline)
            if not fut.done():
                fut.set_result(None)
            await self._settle()
        self._now = target
        await self._settle()


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


# ---------------------------------------------------------------- Channel


class ScriptedChannel:
    """Collects the output events sent by the core."""

    def __init__(self) -> None:
        self.events: list[OutboundEvent] = []
        self._cond = asyncio.Condition()

    async def send(self, event: OutboundEvent) -> None:
        async with self._cond:
            self.events.append(event)
            self._cond.notify_all()

    def of(self, kind: type, key: str | None = None) -> list[Any]:
        return [
            e
            for e in self.events
            if isinstance(e, kind) and (key is None or getattr(e, "conversation_key", key) == key)
        ]

    def kinds(self, key: str | None = None) -> list[str]:
        return [
            type(e).__name__
            for e in self.events
            if key is None or getattr(e, "conversation_key", key) == key
        ]

    async def wait_for(self, kind: type, *, count: int = 1, timeout_seconds: float = 5.0) -> None:
        async def _wait() -> None:
            async with self._cond:
                await self._cond.wait_for(lambda: len(self.of(kind)) >= count)

        await asyncio.wait_for(_wait(), timeout_seconds)


# ---------------------------------------------------------------- Agent engine


@dataclass(frozen=True)
class Text:
    text: str


@dataclass(frozen=True)
class Tool:
    name: str
    summary: str = ""
    result: str = ""
    duration: float = 0.0
    fail: bool = False


@dataclass(frozen=True)
class Wait:
    """Block for `seconds` of (fake) clock time without emitting anything."""

    seconds: float


ScenarioItem = Text | Tool | Wait
Scenario = Sequence[ScenarioItem]


class FakeAgentEngine:
    """Plays a scenario. `scenarios` may be a single scenario (used for every turn)
    or a callable `(request) -> Scenario`."""

    def __init__(
        self,
        clock: Any,
        scenarios: Scenario | Callable[[TurnRequest], Scenario] | None = None,
    ) -> None:
        self._clock = clock
        self._scenarios = scenarios if scenarios is not None else [Text("ok")]
        self._interrupt = asyncio.Event()
        self.requests: list[TurnRequest] = []
        self.appended: list[str] = []
        self._turn_running = False
        self.interrupt_count = 0
        self.reset_count = 0
        self.closed = False
        self.created_at_turn: list[int] = []

    def _scenario(self, request: TurnRequest) -> Scenario:
        if callable(self._scenarios):
            return self._scenarios(request)
        return self._scenarios

    async def _sleep_or_interrupt(self, seconds: float) -> bool:
        """Return True if interrupted."""
        if self._interrupt.is_set():
            return True
        sleeper = asyncio.ensure_future(self._clock.sleep(seconds))
        waiter = asyncio.ensure_future(self._interrupt.wait())
        done, pending = await asyncio.wait({sleeper, waiter}, return_when=asyncio.FIRST_COMPLETED)
        for p in pending:
            p.cancel()
        return waiter in done

    async def run_turn(self, request: TurnRequest) -> AsyncIterator[OutboundEvent]:
        self.requests.append(request)
        self._interrupt = asyncio.Event()
        self._turn_running = True
        try:
            async for event in self._play(request):
                yield event
        finally:
            self._turn_running = False

    async def _play(self, request: TurnRequest) -> AsyncIterator[OutboundEvent]:
        key, turn_id = request.conversation_key, request.turn_id
        tool_n = 0
        for item in self._scenario(request):
            if isinstance(item, Text):
                yield AssistantText(key, turn_id, item.text)
            elif isinstance(item, Wait):
                if await self._sleep_or_interrupt(item.seconds):
                    return
            else:
                tool_n += 1
                tool_id = f"{turn_id}-t{tool_n}"
                started = self._clock.now()
                yield ToolStarted(key, turn_id, tool_id, item.name, item.summary, started)
                interrupted = (
                    await self._sleep_or_interrupt(item.duration) if item.duration else False
                )
                finished = self._clock.now()
                if interrupted:
                    yield ToolFinished(
                        key,
                        turn_id,
                        tool_id,
                        "interrupted",
                        item.summary,
                        finished,
                        (finished - started).total_seconds(),
                    )
                    return
                yield ToolFinished(
                    key,
                    turn_id,
                    tool_id,
                    "failure" if item.fail else "success",
                    item.summary,
                    finished,
                    (finished - started).total_seconds(),
                )
        yield TurnFinished(key, turn_id, "", Usage(), finished_at=self._clock.now())

    async def interrupt(self) -> None:
        self.interrupt_count += 1
        self._interrupt.set()

    async def append(self, text: str) -> bool:
        if not self._turn_running or self._interrupt.is_set():
            return False
        self.appended.append(text)
        return True

    async def reset(self) -> None:
        self.reset_count += 1

    async def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------- Sandbox runtime


@dataclass
class _Workspace:
    files: dict[str, bytes] = field(default_factory=dict)
    notices: list[str] = field(default_factory=list)
    commands: dict[str, CommandResult] = field(default_factory=dict)
    output: dict[str, str] = field(default_factory=dict)
    cancelled: int = 0
    skills: list[str] = field(default_factory=list)


class InMemoryRuntime:
    """Per-conversation in-memory workspace. Commands are scripted, never executed."""

    def __init__(self) -> None:
        self._ws: dict[str, _Workspace] = {}
        self._ids = itertools.count(1)
        self.responses: dict[str, CommandResult] = {}
        self.unknown_commands: set[str] = set()
        self.sent_commands: list[tuple[str, str]] = []
        self.acked: list[str] = []

    def _w(self, key: str) -> _Workspace:
        return self._ws.setdefault(key, _Workspace())

    def add_notice(self, key: str, text: str) -> None:
        self._w(key).notices.append(text)

    async def run_command(
        self,
        conversation_key: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout_seconds: float | None = None,
        on_started: Callable[[str], Awaitable[None]] | None = None,
    ) -> CommandResult:
        self.sent_commands.append((conversation_key, command))
        cid = f"cmd-{next(self._ids)}"
        if on_started is not None:
            await on_started(cid)
        w = self._w(conversation_key)
        if command in self.unknown_commands:
            res = CommandResult(cid, "unknown")
        else:
            base = self.responses.get(command, CommandResult("", "completed", 0))
            res = CommandResult(cid, base.outcome, base.exit_code, base.stdout, base.stderr)
        w.commands[cid] = res
        w.output[cid] = res.stdout + res.stderr
        return res

    async def command_status(self, conversation_key: str, command_id: str) -> CommandResult:
        return self._w(conversation_key).commands.get(
            command_id, CommandResult(command_id, "unknown")
        )

    async def command_output(
        self, conversation_key: str, command_id: str, cursor: int = 0
    ) -> tuple[str, int]:
        out = self._w(conversation_key).output.get(command_id, "")
        return out[cursor:], len(out)

    async def cancel_commands(self, conversation_key: str) -> None:
        self._w(conversation_key).cancelled += 1

    def cancel_count(self, key: str) -> int:
        return self._w(key).cancelled

    async def read_file(self, conversation_key: str, path: str) -> bytes:
        try:
            return self._w(conversation_key).files[path]
        except KeyError:
            raise FileNotFoundError(path) from None

    async def write_file(self, conversation_key: str, path: str, content: bytes) -> None:
        self._w(conversation_key).files[path] = content

    async def list_files(self, conversation_key: str, path: str) -> list[FileEntry]:
        prefix = path.rstrip("/") + "/"
        return [
            FileEntry(p, False, len(c))
            for p, c in sorted(self._w(conversation_key).files.items())
            if p.startswith(prefix) or path in ("", "/", ".")
        ]

    async def is_ready(self, conversation_key: str) -> bool:
        return True

    async def pop_notices(self, conversation_key: str) -> list[str]:
        return list(self._w(conversation_key).notices)

    async def ack_notices(self, conversation_key: str) -> None:
        self._w(conversation_key).notices.clear()
        self.acked.append(conversation_key)

    async def copy_skills(self, conversation_key: str, source: Path, generation: str) -> None:
        self._w(conversation_key).skills.append(generation)


# ---------------------------------------------------------------- State store


class InMemoryStateStore:
    def __init__(self) -> None:
        self.states: dict[str, ConversationState] = {}

    async def get(self, conversation_key: str) -> ConversationState | None:
        s = self.states.get(conversation_key)
        return None if s is None else _copy(s)

    async def put(self, state: ConversationState) -> None:
        self.states[state.conversation_key] = _copy(state)

    async def list_running(self) -> list[ConversationState]:
        return [_copy(s) for s in self.states.values() if s.status == "running"]


def _copy(s: ConversationState) -> ConversationState:
    return ConversationState(
        s.conversation_key,
        s.agent_session_id,
        s.status,
        s.turn_id,
        s.bot_instance_id,
        dict(s.progress_ref),
        list(s.running_command_ids),
        s.updated_at,
    )


# ---------------------------------------------------------------- Guards


class AllowAllGuard:
    async def check(self, text: str, history: list[str]) -> GuardVerdict:
        return GuardVerdict("allow")


class RuleGuard:
    """Blocks messages containing any of the given phrases."""

    def __init__(self, blocked: list[str]) -> None:
        self._blocked = blocked

    async def check(self, text: str, history: list[str]) -> GuardVerdict:
        for b in self._blocked:
            if b in text:
                return GuardVerdict("block", f"'{b}' is not allowed")
        return GuardVerdict("allow")


class NoopSanitizer:
    async def sanitize(self, text: str) -> SanitizeResult:
        return SanitizeResult("pass", text)


class FakeSanitizer:
    """Masks `mask_words`; blocks text containing any `block_words`; can be made to fail."""

    def __init__(
        self,
        mask_words: list[str] | None = None,
        block_words: list[str] | None = None,
        fail: bool = False,
    ) -> None:
        self.mask_words = mask_words or []
        self.block_words = block_words or []
        self.fail = fail
        self.calls: list[str] = []

    async def sanitize(self, text: str) -> SanitizeResult:
        self.calls.append(text)
        if self.fail:
            return SanitizeResult("error", "", "sanitizer failure")
        if any(w in text for w in self.block_words):
            return SanitizeResult("blocked", "", "blocked by policy")
        masked = text
        for w in self.mask_words:
            masked = masked.replace(w, "[MASKED]")
        return SanitizeResult("masked" if masked != text else "pass", masked)
