"""Hooks: observation-only callbacks that receive what happens in a conversation.

A use case implements `Hooks` (usually by extending `BaseHooks`) and lists a factory for it in
the config (`hooks: ["my_pkg.hooks:create"]`). Events carry raw content (user input, tool
input and result, the agent's reply before the output check) so that they can be audited;
masking or dropping content before it leaves the process is up to the hook.

Delivery is asynchronous: `HookDispatcher.publish` only enqueues, so a slow or failing hook
never delays a turn or the other hooks.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import importlib
import json
import logging
import sys
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol

from silk_agent.core.clock import SystemClock
from silk_agent.core.ports import Clock

if TYPE_CHECKING:
    from silk_agent.config import Config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------- events


@dataclass(frozen=True, kw_only=True)
class HookEvent:
    """Fields common to every event. `user` / `user_email` / `channel` are filled in by the
    dispatcher when the source did not know them (e.g. tool events)."""

    kind: str = field(init=False, default="")
    conversation_key: str
    at: datetime
    turn_id: str | None = None
    user: str | None = None
    user_email: str | None = None
    channel: str | None = None  # "slack", "streamlit", ...

    # Names of the fields that hold content (see `json_lines`' `include_content`).
    CONTENT_FIELDS: ClassVar[tuple[str, ...]] = ()


InputDecision = Literal["accepted", "filtered", "stop_word", "blocked", "review", "guard_failed"]


@dataclass(frozen=True, kw_only=True)
class UserInput(HookEvent):
    kind: str = field(init=False, default="user_input")
    text: str
    message_id: str = ""
    attachments: tuple[str, ...] = ()
    decision: InputDecision
    # accepted: ""; blocked / review: the guard's reason; guard_failed: the error.
    reason: str = ""
    # decision == "filtered": the filter that excluded the message.
    filter: str = ""

    CONTENT_FIELDS: ClassVar[tuple[str, ...]] = ("text",)


@dataclass(frozen=True, kw_only=True)
class TurnStart(HookEvent):
    kind: str = field(init=False, default="turn_start")


@dataclass(frozen=True, kw_only=True)
class TurnEnd(HookEvent):
    kind: str = field(init=False, default="turn_end")
    outcome: Literal["completed", "failed", "interrupted"]
    # failed: the error; interrupted: user / new_message / shutdown / crash.
    reason: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float | None = None


@dataclass(frozen=True, kw_only=True)
class ToolStart(HookEvent):
    kind: str = field(init=False, default="tool_start")
    tool_use_id: str
    tool_name: str  # the raw name, e.g. "mcp__sandbox__bash"
    tool_input: Any = None

    CONTENT_FIELDS: ClassVar[tuple[str, ...]] = ("tool_input",)


@dataclass(frozen=True, kw_only=True)
class ToolEnd(HookEvent):
    kind: str = field(init=False, default="tool_end")
    tool_use_id: str
    tool_name: str
    status: Literal["success", "failure", "interrupted", "background"]
    tool_response: Any = None
    duration_seconds: float = 0.0

    CONTENT_FIELDS: ClassVar[tuple[str, ...]] = ("tool_response",)


@dataclass(frozen=True, kw_only=True)
class AgentReply(HookEvent):
    kind: str = field(init=False, default="agent_reply")
    # The agent's text as produced, before the output check.
    text: str
    # pass / masked / blocked / error: what the output check did with it.
    sanitize_status: str = "pass"

    CONTENT_FIELDS: ClassVar[tuple[str, ...]] = ("text",)


@dataclass(frozen=True, kw_only=True)
class SystemNotice(HookEvent):
    kind: str = field(init=False, default="system_notice")
    text: str

    CONTENT_FIELDS: ClassVar[tuple[str, ...]] = ("text",)


# ---------------------------------------------------------------- hook interface


class Hooks(Protocol):
    async def on_event(self, event: HookEvent) -> None: ...

    async def aclose(self) -> None: ...


class BaseHooks:
    """Dispatches to one method per event kind; override the ones you need."""

    async def on_event(self, event: HookEvent) -> None:
        if isinstance(event, UserInput):
            await self.on_user_input(event)
        elif isinstance(event, TurnStart):
            await self.on_turn_start(event)
        elif isinstance(event, TurnEnd):
            await self.on_turn_end(event)
        elif isinstance(event, ToolStart):
            await self.on_tool_start(event)
        elif isinstance(event, ToolEnd):
            await self.on_tool_end(event)
        elif isinstance(event, AgentReply):
            await self.on_agent_reply(event)
        elif isinstance(event, SystemNotice):
            await self.on_system_notice(event)

    async def on_user_input(self, event: UserInput) -> None: ...

    async def on_turn_start(self, event: TurnStart) -> None: ...

    async def on_turn_end(self, event: TurnEnd) -> None: ...

    async def on_tool_start(self, event: ToolStart) -> None: ...

    async def on_tool_end(self, event: ToolEnd) -> None: ...

    async def on_agent_reply(self, event: AgentReply) -> None: ...

    async def on_system_notice(self, event: SystemNotice) -> None: ...

    async def aclose(self) -> None: ...


@dataclass(frozen=True)
class HookContext:
    """What a hook factory receives."""

    options: Mapping[str, Any]
    config: Config
    logger: logging.Logger


class HookLoadError(ValueError):
    """A hook listed in the config could not be loaded (the Bot does not start)."""


def load_hooks(config: Config) -> list[Hooks]:
    """Import and call the factory of each `hooks` entry of the config."""
    hooks: list[Hooks] = []
    for entry in config.hooks:
        factory_name, options = (
            (entry, {}) if isinstance(entry, str) else (entry.factory, dict(entry.options))
        )
        module_name, _, attr = factory_name.partition(":")
        if not module_name or not attr:
            raise HookLoadError(f"hook {factory_name!r}: expected 'package.module:factory'")
        try:
            module = importlib.import_module(module_name)
            factory = getattr(module, attr)
        except ImportError as e:
            raise HookLoadError(f"hook {factory_name!r}: cannot import {module_name!r}: {e}") from e
        except AttributeError as e:
            raise HookLoadError(f"hook {factory_name!r}: {module_name!r} has no {attr!r}") from e
        try:
            hook = factory(HookContext(options, config, logging.getLogger(module_name)))
        except Exception as e:
            raise HookLoadError(f"hook {factory_name!r}: the factory failed: {e}") from e
        if not hasattr(hook, "on_event") or not hasattr(hook, "aclose"):
            raise HookLoadError(f"hook {factory_name!r}: the factory did not return Hooks")
        hooks.append(hook)
    return hooks


# ---------------------------------------------------------------- dispatcher

DEFAULT_QUEUE_SIZE = 10_000
DEFAULT_HOOK_TIMEOUT_SECONDS = 10.0


class HookDispatcher:
    """Queues events and hands them, in order, to every hook from one worker task."""

    def __init__(
        self,
        hooks: Sequence[Hooks],
        clock: Clock | None = None,
        *,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        hook_timeout_seconds: float = DEFAULT_HOOK_TIMEOUT_SECONDS,
    ) -> None:
        self._hooks = list(hooks)
        self._clock = clock or SystemClock()
        self._queue_size = max(1, queue_size)
        self._timeout = hook_timeout_seconds
        self._queue: deque[HookEvent] = deque()
        self._wake = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._worker: asyncio.Task[None] | None = None
        self._closed = False
        self._senders: dict[str, tuple[str | None, str | None]] = {}
        self.dropped = 0

    def __bool__(self) -> bool:
        return bool(self._hooks)

    # ------------------------------------------------------------ publish

    def publish(self, event: HookEvent) -> None:
        """Enqueue `event` (never waits, never raises). The oldest events are dropped when
        the queue is full."""
        if self._closed or not self._hooks:
            return
        try:
            event = self._complete(event)
            if len(self._queue) >= self._queue_size:
                self._queue.popleft()
                self.dropped += 1
                if self.dropped == 1 or self.dropped % 1000 == 0:
                    log.warning(
                        "hook queue is full (%d): dropped %d event(s) so far",
                        self._queue_size,
                        self.dropped,
                    )
            self._queue.append(event)
            self._idle.clear()
            self._wake.set()
            if self._worker is None or self._worker.done():
                self._worker = asyncio.get_running_loop().create_task(
                    self._run(), name="hook-dispatcher"
                )
        except Exception:
            log.exception("could not queue a hook event")

    def _complete(self, event: HookEvent) -> HookEvent:
        """Add the sender and channel, which tool / turn events do not know themselves."""
        key = event.conversation_key
        if isinstance(event, UserInput) and event.user:
            self._senders[key] = (event.user, event.user_email)
        changes: dict[str, Any] = {}
        if event.user is None and key in self._senders:
            changes["user"], changes["user_email"] = self._senders[key]
        if event.channel is None:
            changes["channel"] = key.split(":", 1)[0]
        return dataclasses.replace(event, **changes) if changes else event

    # ------------------------------------------------------------ delivery

    async def _run(self) -> None:
        while True:
            if not self._queue:
                self._idle.set()
                self._wake.clear()
                await self._wake.wait()
                continue
            event = self._queue.popleft()
            for hook in self._hooks:
                await self._call(hook, hook.on_event(event), "on_event", event.kind)

    async def _call(self, hook: Hooks, awaitable: Awaitable[Any], what: str, kind: str) -> None:
        """Run one hook call; an exception or a time out is logged and delivery goes on."""
        name = type(hook).__name__
        task = asyncio.ensure_future(awaitable)
        timer = asyncio.ensure_future(self._clock.sleep(self._timeout))
        try:
            await asyncio.wait({task, timer}, return_when=asyncio.FIRST_COMPLETED)
            if task.done():
                if (exc := task.exception()) is not None:
                    log.error("hook %s.%s failed on %s", name, what, kind, exc_info=exc)
            else:
                log.error(
                    "hook %s.%s took more than %.0f s on %s; skipped",
                    name,
                    what,
                    self._timeout,
                    kind,
                )
        finally:
            for t in (task, timer):
                if not t.done():
                    t.cancel()
            await asyncio.gather(task, timer, return_exceptions=True)

    async def aclose(self, timeout_seconds: float = 10.0) -> None:
        """Deliver what is queued (within `timeout_seconds`), then close every hook."""
        self._closed = True
        if self._worker is not None:
            idle = asyncio.ensure_future(self._idle.wait())
            timer = asyncio.ensure_future(self._clock.sleep(timeout_seconds))
            try:
                await asyncio.wait({idle, timer}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for t in (idle, timer):
                    t.cancel()
                await asyncio.gather(idle, timer, return_exceptions=True)
            if self._queue:
                log.warning("hook shutdown: %d event(s) were not delivered", len(self._queue))
                self._queue.clear()
            self._worker.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._worker
        for hook in self._hooks:
            await self._call(hook, hook.aclose(), "aclose", "close")
        if self.dropped:
            log.warning("hooks dropped %d event(s) because the queue was full", self.dropped)


# ---------------------------------------------------------------- standard hook


class JsonLinesHooks(BaseHooks):
    """One JSON object per line on stdout (Cloud Logging collects it as a structured log)."""

    def __init__(
        self, *, include_content: bool = False, write: Callable[[str], None] | None = None
    ) -> None:
        self._include_content = include_content
        self._write = write or _write_stdout

    async def on_event(self, event: HookEvent) -> None:
        data = dataclasses.asdict(event)
        if not self._include_content:
            for name in event.CONTENT_FIELDS:
                data.pop(name, None)
        self._write(json.dumps(data, ensure_ascii=False, default=_json_default))


def _write_stdout(line: str) -> None:
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def json_lines(ctx: HookContext) -> Hooks:
    """Factory `silk_agent.hooks:json_lines`; option `include_content` (default false)."""
    return JsonLinesHooks(include_content=bool(ctx.options.get("include_content", False)))


# Referenced by type checkers / re-exports.
__all__ = [
    "AgentReply",
    "BaseHooks",
    "HookContext",
    "HookDispatcher",
    "HookEvent",
    "HookLoadError",
    "Hooks",
    "JsonLinesHooks",
    "SystemNotice",
    "ToolEnd",
    "ToolStart",
    "TurnEnd",
    "TurnStart",
    "UserInput",
    "json_lines",
    "load_hooks",
]
