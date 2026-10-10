"""Turns Slack event payloads into core UserMessages, deciding whether the Bot reacts."""

from __future__ import annotations

import logging
import re
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any

from silk_agent.core.events import InboundEvent, UserMessage

log = logging.getLogger(__name__)

DM_THREAD = "-"
_MENTION = re.compile(r"<@[A-Z0-9]+(\|[^>]*)?>")


def make_key(team: str, channel: str, thread_ts: str | None) -> str:
    return f"slack:{team}:{channel}:{thread_ts or DM_THREAD}"


def parse_key(key: str) -> tuple[str, str, str | None]:
    """`slack:T:C:ts` -> (team, channel, thread_ts or None for a DM conversation)."""
    _, team, channel, thread = key.split(":", 3)
    return team, channel, None if thread == DM_THREAD else thread


class RecentIds:
    """Bounded set for de-duplicating re-delivered events."""

    def __init__(self, limit: int = 5000) -> None:
        self._limit = limit
        self._ids: OrderedDict[str, None] = OrderedDict()

    def seen(self, value: str) -> bool:
        """True if `value` was already seen; otherwise remember it."""
        if value in self._ids:
            return True
        self._ids[value] = None
        if len(self._ids) > self._limit:
            self._ids.popitem(last=False)
        return False


class SlackEventRouter:
    """Reacts to: mentions in channels, replies in threads the Bot takes part in, and DMs."""

    def __init__(
        self,
        handle: Callable[[InboundEvent], Awaitable[None]],
        *,
        bot_user_id: str,
        is_participating: Callable[[str], Awaitable[bool]],
        mark_participating: Callable[[str], None] | None = None,
    ) -> None:
        self._handle = handle
        self._bot_user_id = bot_user_id
        self._is_participating = is_participating
        self._mark = mark_participating
        self._event_ids = RecentIds()
        self._messages = RecentIds()

    # ------------------------------------------------------------ entry points

    async def on_app_mention(self, body: dict[str, Any]) -> None:
        event = body.get("event", {})
        if not self._first_delivery(body, event):
            return
        team = body.get("team_id") or event.get("team", "")
        thread_ts = event.get("thread_ts") or event["ts"]
        key = make_key(team, event["channel"], thread_ts)
        if self._mark is not None:
            self._mark(key)
        await self._emit(key, team, event)

    async def on_message(self, body: dict[str, Any]) -> None:
        event = body.get("event", {})
        if event.get("user") == self._bot_user_id:
            return  # the Bot's own posts
        team = body.get("team_id") or event.get("team", "")
        channel = event.get("channel", "")

        if event.get("channel_type") == "im":
            if not self._first_delivery(body, event):
                return
            key = make_key(team, channel, event.get("thread_ts"))
            await self._emit(key, team, event)
            return

        thread_ts = event.get("thread_ts")
        if not thread_ts or thread_ts == event.get("ts"):
            return  # top-level channel message: only a mention (app_mention) starts a conversation
        key = make_key(team, channel, thread_ts)
        if not await self._is_participating(key):
            return
        if not self._first_delivery(body, event):
            return
        await self._emit(key, team, event)

    # ------------------------------------------------------------ helpers

    def _first_delivery(self, body: dict[str, Any], event: dict[str, Any]) -> bool:
        """False for a re-delivered event (same event_id) or an already handled message
        (the same post arrives as both `app_mention` and `message`)."""
        event_id = body.get("event_id")
        if event_id and self._event_ids.seen(event_id):
            return False
        message_id = f"{event.get('channel', '')}:{event.get('ts', '')}"
        return not self._messages.seen(message_id)

    async def _emit(self, key: str, team: str, event: dict[str, Any]) -> None:
        text = _MENTION.sub("", event.get("text", "")).strip()
        message = UserMessage(
            conversation_key=key,
            user=event.get("user", ""),
            text=text,
            message_id=f"{event.get('channel', '')}:{event.get('ts', '')}",
            attachments=tuple(f.get("name", "") for f in event.get("files", []) or []),
            team=team,
            channel=event.get("channel", ""),
            sender_is_bot=bool(event.get("bot_id")),
            kind=event.get("subtype") or "message",
        )
        await self._handle(message)
