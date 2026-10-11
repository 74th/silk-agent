"""Channel-independent input / output events."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

# ---------------------------------------------------------------- inbound


@dataclass(frozen=True)
class UserMessage:
    conversation_key: str
    user: str
    text: str
    message_id: str = ""
    attachments: tuple[str, ...] = ()
    # Source information used by MessageFilter. Channels that do not have these
    # concepts leave the defaults.
    team: str = ""
    channel: str = ""
    sender_is_bot: bool = False
    # "message" for a normal user post; anything else (edit, join, ...) is a system event.
    kind: str = "message"


@dataclass(frozen=True)
class StopRequest:
    conversation_key: str
    user: str = ""


@dataclass(frozen=True)
class ResetRequest:
    conversation_key: str
    user: str = ""


@dataclass(frozen=True)
class ResumeRequest:
    conversation_key: str
    user: str = ""


InboundEvent = UserMessage | StopRequest | ResetRequest | ResumeRequest

# ---------------------------------------------------------------- outbound

# background: the tool call returned while its command keeps running in the sandbox
ToolStatus = Literal["running", "success", "failure", "interrupted", "background"]
TurnStatus = Literal["thinking", "running_tool", "interrupting", "interrupted"]
InterruptReason = Literal["crash", "shutdown", "user", "new_message"]


@dataclass(frozen=True)
class TurnStarted:
    conversation_key: str
    turn_id: str
    started_at: datetime


@dataclass(frozen=True)
class AssistantText:
    conversation_key: str
    turn_id: str
    text: str


@dataclass(frozen=True)
class ToolStarted:
    conversation_key: str
    turn_id: str
    tool_use_id: str
    name: str
    summary: str
    started_at: datetime


@dataclass(frozen=True)
class ToolFinished:
    conversation_key: str
    turn_id: str
    tool_use_id: str
    status: ToolStatus
    summary: str
    finished_at: datetime
    duration_seconds: float = 0.0


@dataclass(frozen=True)
class Heartbeat:
    conversation_key: str
    turn_id: str
    at: datetime


@dataclass(frozen=True)
class StatusChanged:
    conversation_key: str
    turn_id: str
    status: TurnStatus


@dataclass(frozen=True)
class MessageAppended:
    """A user message was added to the running turn (it is answered within that turn)."""

    conversation_key: str
    turn_id: str


@dataclass(frozen=True)
class GuardBlocked:
    conversation_key: str
    stage: Literal["input", "output"]
    reason: str


@dataclass(frozen=True)
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float | None = None


@dataclass(frozen=True)
class TurnFinished:
    conversation_key: str
    turn_id: str
    result: str = ""
    usage: Usage = field(default_factory=Usage)
    warnings: tuple[str, ...] = ()
    finished_at: datetime | None = None


@dataclass(frozen=True)
class TurnFailed:
    conversation_key: str
    turn_id: str
    error: str


@dataclass(frozen=True)
class TurnInterrupted:
    """The turn ended without finishing (user stop, new message, shutdown, crash)."""

    conversation_key: str
    turn_id: str
    reason: InterruptReason
    at: datetime | None = None


@dataclass(frozen=True)
class Notice:
    """Plain message to the user that is not part of a turn (e.g. nothing to stop)."""

    conversation_key: str
    text: str


OutboundEvent = (
    TurnStarted
    | AssistantText
    | ToolStarted
    | ToolFinished
    | Heartbeat
    | StatusChanged
    | MessageAppended
    | GuardBlocked
    | TurnFinished
    | TurnFailed
    | TurnInterrupted
    | Notice
)
