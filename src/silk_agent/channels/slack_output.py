"""Outbound side of the Slack adapter: progress message, replies, uploads."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from silk_agent.channels.slack_events import parse_key
from silk_agent.channels.slack_progress import RESUME_HINT, ProgressView, format_stamp
from silk_agent.config import ProgressConfig
from silk_agent.core.events import (
    AssistantText,
    GuardBlocked,
    Heartbeat,
    Notice,
    OutboundEvent,
    StatusChanged,
    ToolFinished,
    ToolStarted,
    TurnFailed,
    TurnFinished,
    TurnInterrupted,
    TurnStarted,
)
from silk_agent.core.ports import Clock, ConversationState, SessionStateStore

log = logging.getLogger(__name__)

MAX_MESSAGE_CHARS = 12_000
UPDATE_INTERVAL_SECONDS = 1.0
_STAMP = re.compile(r"最終更新 (\d{4}-\d{2}-\d{2} \d{2}:\d{2})")


@dataclass
class _Conv:
    key: str
    channel: str
    thread_ts: str | None
    view: ProgressView
    ts: str = ""
    last_text: str = ""
    last_flush: datetime | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    flush_task: asyncio.Task[None] | None = None


class SlackOutput:
    def __init__(
        self,
        client: Any,
        clock: Clock,
        progress: ProgressConfig,
        *,
        state_store: SessionStateStore | None = None,
        max_message_chars: int = MAX_MESSAGE_CHARS,
        update_interval: float = UPDATE_INTERVAL_SECONDS,
    ) -> None:
        self._client = client
        self._clock = clock
        self._cfg = progress
        self._state_store = state_store
        self._max_chars = max_message_chars
        self._interval = update_interval
        self._convs: dict[str, _Conv] = {}

    # ------------------------------------------------------------ dispatch

    async def send(self, event: OutboundEvent) -> None:
        try:
            await self._send(event)
        except Exception:
            log.exception("slack output failed for %s", type(event).__name__)

    async def _send(self, event: OutboundEvent) -> None:
        key = event.conversation_key if hasattr(event, "conversation_key") else ""
        if isinstance(event, TurnStarted):
            await self._start_turn(event)
        elif isinstance(event, ToolStarted | ToolFinished | StatusChanged | Heartbeat):
            conv = self._convs.get(key)
            if conv is not None:
                conv.view.apply(event)
                await self._schedule_flush(conv)
        elif isinstance(event, TurnFinished | TurnFailed):
            conv = self._convs.get(key)
            if conv is not None:
                conv.view.apply(event)
                await self._flush_now(conv)
        elif isinstance(event, TurnInterrupted):
            await self._interrupted(event)
        elif isinstance(event, AssistantText):
            await self._reply(event.conversation_key, event.text)
        elif isinstance(event, GuardBlocked):
            prefix = "⚠️ 入力を処理しませんでした: " if event.stage == "input" else "⚠️ "
            await self._reply(event.conversation_key, prefix + event.reason)
        elif isinstance(event, Notice):
            await self._reply(event.conversation_key, event.text)

    # ------------------------------------------------------------ progress message

    def _target(self, key: str) -> tuple[str, str | None]:
        _, channel, thread = parse_key(key)
        return channel, thread

    async def _start_turn(self, event: TurnStarted) -> None:
        old = self._convs.get(event.conversation_key)
        if old is not None:
            await self._cancel_flush(old)
        channel, thread = self._target(event.conversation_key)
        view = ProgressView(
            event.started_at,
            self._cfg.timezone,
            self._cfg.max_tools_shown,
            self._cfg.tool_display,
        )
        conv = _Conv(event.conversation_key, channel, thread, view)
        self._convs[event.conversation_key] = conv
        text = view.render(self._clock.now())
        conv.ts = await self._post(channel, thread, text)
        conv.last_text, conv.last_flush = text, self._clock.now()
        await self._save_ref(conv)

    async def _schedule_flush(self, conv: _Conv) -> None:
        """Updates are batched to at most one `chat.update` per interval."""
        if conv.flush_task is not None and not conv.flush_task.done():
            return  # the pending flush renders the newest state
        now = self._clock.now()
        wait = 0.0
        if conv.last_flush is not None:
            wait = max(0.0, self._interval - (now - conv.last_flush).total_seconds())
        if wait <= 0:
            await self._flush(conv)
            return

        async def later() -> None:
            await self._clock.sleep(wait)
            await self._flush(conv)

        conv.flush_task = asyncio.create_task(later())

    async def _flush_now(self, conv: _Conv) -> None:
        await self._cancel_flush(conv)
        await self._flush(conv)

    @staticmethod
    async def _cancel_flush(conv: _Conv) -> None:
        task = conv.flush_task
        if task is not None and not task.done() and task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        conv.flush_task = None

    async def _flush(self, conv: _Conv) -> None:
        async with conv.lock:
            now = self._clock.now()
            text = conv.view.render(now)
            if text == conv.last_text:
                return
            try:
                await self._client.chat_update(channel=conv.channel, ts=conv.ts, text=text)
            except Exception:
                # The message may have been deleted: post a new progress message instead.
                log.warning("chat.update failed for %s; reposting", conv.key, exc_info=True)
                conv.ts = await self._post(conv.channel, conv.thread_ts, text)
                await self._save_ref(conv)
            conv.last_text, conv.last_flush = text, now

    async def _interrupted(self, event: TurnInterrupted) -> None:
        conv = self._convs.get(event.conversation_key)
        if conv is not None:
            conv.view.apply(event)
            await self._flush_now(conv)
            if event.reason == "shutdown":
                await self._reply(
                    event.conversation_key, f"Botの再起動により中断しました。{RESUME_HINT}。"
                )
            return
        if event.reason == "crash":
            await self._mark_crashed(event)

    # ------------------------------------------------------------ after a restart

    async def _mark_crashed(self, event: TurnInterrupted) -> None:
        """The Bot died during this turn: rewrite the old progress message from the stored
        position and tell the thread how to resume."""
        key = event.conversation_key
        state = await self._get_state(key)
        ref = state.progress_ref if state else {}
        tz = ZoneInfo(self._cfg.timezone)
        fallback_at = event.at or self._clock.now()
        if ref.get("ts"):
            old_text = await self._fetch_text(ref["channel"], ref["ts"])
            text = rewrite_as_crashed(old_text, format_stamp(fallback_at, tz))
            try:
                await self._client.chat_update(channel=ref["channel"], ts=ref["ts"], text=text)
            except Exception:
                log.warning("could not rewrite the progress message of %s", key, exc_info=True)
        await self._reply(key, f"Botの停止によりこの処理は中断されました。{RESUME_HINT}。")

    async def _fetch_text(self, channel: str, ts: str) -> str:
        try:
            res = await self._client.conversations_history(
                channel=channel, latest=ts, inclusive=True, limit=1
            )
            messages = res["messages"]
            return messages[0].get("text", "") if messages else ""
        except Exception:
            log.warning("could not read the old progress message", exc_info=True)
            return ""

    # ------------------------------------------------------------ plain messages

    async def _post(self, channel: str, thread_ts: str | None, text: str) -> str:
        kwargs: dict[str, Any] = {"channel": channel, "text": text}
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        res = await self._client.chat_postMessage(**kwargs)
        return res["ts"]

    async def _reply(self, key: str, text: str) -> None:
        if not text.strip():
            return
        channel, thread = self._target(key)
        if len(text) > self._max_chars:
            kwargs: dict[str, Any] = {
                "channel": channel,
                "content": text,
                "filename": "response.md",
                "title": "応答（長いためファイルにしました）",
            }
            if thread:
                kwargs["thread_ts"] = thread
            await self._client.files_upload_v2(**kwargs)
            return
        await self._post(channel, thread, text)

    # ------------------------------------------------------------ state

    async def _get_state(self, key: str) -> ConversationState | None:
        if self._state_store is None:
            return None
        try:
            return await self._state_store.get(key)
        except Exception:
            log.exception("could not read state for %s", key)
            return None

    async def _save_ref(self, conv: _Conv) -> None:
        if self._state_store is None:
            return
        try:
            state = await self._state_store.get(conv.key) or ConversationState(conv.key)
            state.progress_ref = {
                "channel": conv.channel,
                "ts": conv.ts,
                "thread_ts": conv.thread_ts,
            }
            await self._state_store.put(state)
        except Exception:
            log.exception("could not save the progress message position for %s", conv.key)


def rewrite_as_crashed(old_text: str, fallback_stamp: str) -> str:
    """Turn the last rendering of a progress message into the "interrupted by Bot stop"
    form, keeping the tool list and the last "最終更新" time that the Bot managed to write."""
    match = _STAMP.search(old_text)
    stamp = match.group(1) if match else fallback_stamp
    lines = old_text.splitlines()[1:] if old_text else []
    body = [
        line.replace("⏳ 実行中", "⏹ 中断", 1) if line.startswith("⏳ 実行中") else line
        for line in lines
        if not line.startswith("停止するには")
    ]
    return "\n".join([f"⚠️ Botの停止により中断（最終更新 {stamp}）", *body, RESUME_HINT])
