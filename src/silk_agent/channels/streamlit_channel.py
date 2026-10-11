"""StreamlitChannel: an event log per conversation plus a UI-independent view model."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Literal

from silk_agent.channels.slack_progress import format_duration
from silk_agent.core.events import (
    AssistantText,
    GuardBlocked,
    Notice,
    OutboundEvent,
    ToolFinished,
    ToolStarted,
    TurnFailed,
    TurnFinished,
    TurnInterrupted,
    TurnStarted,
)


@dataclass(frozen=True)
class UserEntry:
    text: str
    at: float = field(default_factory=time.monotonic)


LogEntry = UserEntry | OutboundEvent

_TERMINAL = (TurnFinished, TurnFailed, TurnInterrupted)


class StreamlitChannel:
    """Collects events in a thread-safe, append-only log. The Streamlit script re-runs on
    every interaction, so the UI always renders from the log (never from live objects)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._logs: dict[str, list[LogEntry]] = {}

    async def send(self, event: OutboundEvent) -> None:
        key = getattr(event, "conversation_key", "")
        with self._lock:
            self._logs.setdefault(key, []).append(event)

    def add_user_message(self, key: str, text: str) -> None:
        with self._lock:
            self._logs.setdefault(key, []).append(UserEntry(text))

    def log(self, key: str) -> list[LogEntry]:
        with self._lock:
            return list(self._logs.get(key, []))

    def is_running(self, key: str) -> bool:
        """True while a turn has started and has not reached a terminal event."""
        running = False
        for entry in self.log(key):
            if isinstance(entry, TurnStarted):
                running = True
            elif isinstance(entry, _TERMINAL):
                running = False
        return running

    def is_busy(self, key: str, pending_seconds: float = 30.0) -> bool:
        """True while the UI should keep refreshing: a turn is running, or a message was just
        sent and is still in the filter / input guard (no event has answered it yet)."""
        if self.is_running(key):
            return True
        entries = self.log(key)
        if entries and isinstance(entries[-1], UserEntry):
            return time.monotonic() - entries[-1].at < pending_seconds
        return False


# ---------------------------------------------------------------- view model

ToolState = Literal["running", "success", "failure", "interrupted", "background"]


@dataclass
class ToolItem:
    tool_use_id: str
    name: str
    summary: str
    state: ToolState = "running"
    duration: str = ""


@dataclass
class ViewItem:
    kind: Literal["user", "assistant", "tools", "notice", "status"]
    text: str = ""
    tools: list[ToolItem] = field(default_factory=list)
    state: str = ""  # for "tools": running | complete | error | interrupted


def build_view(entries: list[LogEntry]) -> list[ViewItem]:
    """Group the log into chat items: user / assistant text / one tool box per run of tools."""
    items: list[ViewItem] = []
    current: ViewItem | None = None  # the open tool box

    def close_tools(state: str) -> None:
        nonlocal current
        if current is not None:
            current.state = state
            current = None

    for entry in entries:
        if isinstance(entry, UserEntry):
            items.append(ViewItem("user", entry.text))
        elif isinstance(entry, ToolStarted):
            if current is None:
                current = ViewItem("tools", state="running")
                items.append(current)
            current.tools.append(ToolItem(entry.tool_use_id, entry.name, entry.summary))
        elif isinstance(entry, ToolFinished):
            for box in items:
                for t in box.tools:
                    if t.tool_use_id == entry.tool_use_id:
                        t.state = entry.status
                        t.duration = format_duration(entry.duration_seconds)
        elif isinstance(entry, AssistantText):
            close_tools("complete")
            items.append(ViewItem("assistant", entry.text))
        elif isinstance(entry, GuardBlocked):
            close_tools("complete")
            prefix = "入力を処理しませんでした: " if entry.stage == "input" else ""
            items.append(ViewItem("notice", prefix + entry.reason))
        elif isinstance(entry, Notice):
            close_tools("complete")
            items.append(ViewItem("notice", entry.text))
        elif isinstance(entry, TurnFinished):
            close_tools("complete")
            for w in entry.warnings:
                items.append(ViewItem("notice", w))
            items.append(ViewItem("status", "✅ 完了"))
        elif isinstance(entry, TurnFailed):
            close_tools("error")
            items.append(ViewItem("status", f"❌ 失敗: {entry.error}"))
        elif isinstance(entry, TurnInterrupted):
            close_tools("interrupted")
            label = {
                "user": "⏹ 中断しました",
                "new_message": "↪ 新しい指示で再開",
                "shutdown": "⚠️ 再起動により中断",
                "crash": "⚠️ 停止により中断",
            }[entry.reason]
            items.append(ViewItem("status", label))
    return items
