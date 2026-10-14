"""Authorization of stop/reset candidates from verified sender information."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from silk_agent.config import Config
from silk_agent.core.events import SenderInfo, UserMessage
from silk_agent.core.filters import default_chain
from silk_agent.core.ports import Clock


def build_control_authorizer(config: Config, clock: Clock) -> Callable[[Mapping[str, Any]], bool]:
    """Apply the same sender/allow-list policy as normal input (no per-user rate limit).

    A candidate is only evaluated after sender enrichment resolved, so a lookup failure never
    reaches this function as an allowed user.
    """
    chain = default_chain(
        clock,
        teams=config.filter.allowed_teams,
        channels=config.filter.allowed_channels,
        users=config.filter.allowed_users,
        allow_direct_messages=config.filter.allow_direct_messages,
        deny_guests=config.filter.deny_guests,
        deny_external_users=config.filter.deny_external_users,
        max_chars=config.filter.max_message_chars,
        per_minute=0,
    )

    def authorize(record: Mapping[str, Any]) -> bool:
        payload = record.get("payload")
        if not isinstance(payload, Mapping):
            return False
        raw = payload.get("sender")
        if not isinstance(raw, Mapping):
            return False
        user = payload.get("user")
        if not isinstance(user, str) or not user:
            return False
        sender = SenderInfo(
            email=str(raw.get("email") or ""),
            is_guest=raw["is_guest"] if isinstance(raw.get("is_guest"), bool) else None,
            is_external=raw["is_external"] if isinstance(raw.get("is_external"), bool) else None,
            team=str(raw.get("team") or ""),
        )
        message = UserMessage(
            conversation_key=str(record.get("conversation_key", "")),
            user=user,
            text=str(payload.get("text") or ""),
            team=str(payload.get("team") or ""),
            channel=str(payload.get("channel") or ""),
            sender_is_bot=payload.get("sender_is_bot") is True,
            is_direct=payload.get("is_direct") is True,
            sender=sender,
            kind="message",
        )
        return chain.check(message, skip_rate_limit=True).allowed

    return authorize
