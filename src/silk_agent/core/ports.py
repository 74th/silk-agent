"""Ports: everything the core needs from the outside world."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable

from silk_agent.core.events import OutboundEvent

# ---------------------------------------------------------------- Clock


class Clock(Protocol):
    def now(self) -> datetime: ...

    async def sleep(self, seconds: float) -> None: ...


# ---------------------------------------------------------------- ChatChannel


class ChatChannel(Protocol):
    """Delivers output events to the user. Inbound events are pushed to the
    orchestrator by the channel itself."""

    async def send(self, event: OutboundEvent) -> None: ...


# ---------------------------------------------------------------- AgentEngine


@dataclass(frozen=True)
class TurnRequest:
    conversation_key: str
    turn_id: str
    text: str
    # Prepended to the user text as <system-notice> (sandbox notices, resume info).
    notices: tuple[str, ...] = ()
    # Claude session to resume when the engine has to (re)connect; None = start a new one.
    resume_session_id: str | None = None
    input_id: str = ""
    dispatch_attempt_id: str = ""


class AgentEngine(Protocol):
    """One engine instance serves one conversation (one ClaudeSDKClient)."""

    def run_turn(self, request: TurnRequest) -> AsyncIterator[OutboundEvent]:
        """Yield AssistantText / ToolStarted / ToolFinished and finally TurnFinished
        (or TurnFailed). After interrupt() the iterator just ends (running tools are
        reported as ToolFinished(status="interrupted")); the caller emits TurnInterrupted."""
        ...

    async def interrupt(self) -> None:
        """Stop the running turn. The iterator from run_turn then ends."""
        ...

    async def append(self, text: str) -> bool:
        """Add a user message to the running turn without stopping it (the model reads it
        after the running tool finishes). False when no turn can take it; the caller then
        queues the message as a new turn."""
        ...

    async def reset(self) -> None:
        """Forget the conversation history; the next turn starts a new session."""
        ...

    async def close(self) -> None: ...


AgentEngineFactory = Callable[[str], AgentEngine]


# ---------------------------------------------------------------- SandboxRuntime

CommandOutcome = Literal["completed", "running", "unknown", "cancelled"]


@dataclass(frozen=True)
class CommandResult:
    command_id: str
    outcome: CommandOutcome
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""


@dataclass(frozen=True)
class FileEntry:
    path: str
    is_dir: bool = False
    size: int = 0


class SandboxRuntime(Protocol):
    async def run_command(
        self,
        conversation_key: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout_seconds: float | None = None,
        on_started: Callable[[str], Awaitable[None]] | None = None,
    ) -> CommandResult:
        """Never re-sends a command. If the result is lost, outcome="unknown".
        `on_started(command_id)` is awaited as soon as the command id is known."""
        ...

    async def command_status(self, conversation_key: str, command_id: str) -> CommandResult: ...

    async def command_output(
        self, conversation_key: str, command_id: str, cursor: int = 0
    ) -> tuple[str, int]:
        """Return (output after cursor, new cursor)."""
        ...

    async def cancel_commands(self, conversation_key: str) -> None: ...

    async def read_file(self, conversation_key: str, path: str) -> bytes: ...

    async def write_file(self, conversation_key: str, path: str, content: bytes) -> None: ...

    async def list_files(self, conversation_key: str, path: str) -> list[FileEntry]: ...

    async def pop_notices(self, conversation_key: str) -> list[str]:
        """Notices (e.g. workspace deleted) that should reach the agent. Makes sure the
        sandbox is running (creates / resumes it), which can take a while."""
        ...

    async def is_ready(self, conversation_key: str) -> bool:
        """True when the conversation's sandbox is already running (no start-up wait)."""
        ...

    async def ack_notices(self, conversation_key: str) -> None: ...

    async def copy_skills(self, conversation_key: str, source: Path, generation: str) -> None: ...


# ---------------------------------------------------------------- SessionStateStore

RunStatus = Literal["idle", "running", "interrupted_crash", "interrupted_shutdown"]


@dataclass
class ConversationState:
    conversation_key: str
    agent_session_id: str | None = None
    status: RunStatus = "idle"
    turn_id: str | None = None
    bot_instance_id: str | None = None
    # Channel-specific location of the progress view (Slack: channel / ts).
    progress_ref: dict[str, Any] = field(default_factory=dict)
    running_command_ids: list[str] = field(default_factory=list)
    updated_at: datetime | None = None
    # Who started the conversation (set once, when its first message is accepted). `None`
    # for conversations that were started before this was recorded.
    owner_user: str | None = None
    owner_email: str | None = None
    progress_snapshot: dict[str, Any] = field(default_factory=dict)


# Environment variable that carries the e-mail address of the user who started the
# conversation into the Sandbox (container env, so startup.sh and agent commands see it).
AGENT_USER_EMAIL_ENV = "AGENT_USER_EMAIL"


class SessionStateStore(Protocol):
    async def get(self, conversation_key: str) -> ConversationState | None: ...

    async def put(self, state: ConversationState) -> None: ...

    async def list_running(self) -> list[ConversationState]: ...


# ---------------------------------------------------------------- Guards


@dataclass(frozen=True)
class GuardVerdict:
    decision: Literal["allow", "block", "review"]
    reason: str = ""


class InputGuard(Protocol):
    async def check(self, text: str, history: list[str]) -> GuardVerdict:
        """Raises on infrastructure failure; the caller applies on_error."""
        ...


@dataclass(frozen=True)
class SanitizeResult:
    status: Literal["pass", "masked", "blocked", "error"]
    text: str = ""
    reason: str = ""


class OutputSanitizer(Protocol):
    async def sanitize(self, text: str) -> SanitizeResult: ...


# ---------------------------------------------------------------- Skills


@dataclass(frozen=True)
class SkillGeneration:
    generation_id: str
    # Directory that contains `.claude/skills/`.
    root: Path


@runtime_checkable
class SkillSource(Protocol):
    async def refresh(self) -> SkillGeneration | None:
        """Sync from the source; returns the current generation (None if no skills)."""
        ...

    def current(self) -> SkillGeneration | None: ...


# Hook used by the orchestrator to push events to the channel.
EventSink = Callable[[OutboundEvent], Awaitable[None]]
