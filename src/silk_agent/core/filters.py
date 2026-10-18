"""Rule-based MessageFilter chain (runs before the Gemini guard)."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Protocol

from silk_agent.core.events import UserMessage
from silk_agent.core.ports import Clock


@dataclass(frozen=True)
class FilterDecision:
    allowed: bool
    # Shown to the user when set; None means "ignore silently".
    reply: str | None = None
    # Name of the filter that excluded the message (set by FilterChain).
    reason: str = ""


ALLOW = FilterDecision(True)


class MessageFilter(Protocol):
    def check(self, message: UserMessage) -> FilterDecision: ...


class FilterChain:
    def __init__(self, filters: Sequence[MessageFilter]) -> None:
        self._filters = list(filters)

    def check(self, message: UserMessage, *, skip_rate_limit: bool = False) -> FilterDecision:
        for f in self._filters:
            if skip_rate_limit and isinstance(f, RateLimitFilter):
                continue
            decision = f.check(message)
            if not decision.allowed:
                return replace(decision, reason=type(f).__name__)
        return ALLOW


class SenderFilter:
    """Drops bots and non-message (system) events."""

    def check(self, message: UserMessage) -> FilterDecision:
        if message.sender_is_bot or message.kind != "message":
            return FilterDecision(False)
        return ALLOW


class AllowListFilter:
    """Empty list = allow all. The channel list does not apply to direct messages (see
    DirectMessageFilter)."""

    def __init__(
        self,
        teams: Sequence[str] = (),
        channels: Sequence[str] = (),
        users: Sequence[str] = (),
    ) -> None:
        self._teams, self._channels, self._users = set(teams), set(channels), set(users)

    def check(self, message: UserMessage) -> FilterDecision:
        if self._teams and message.team not in self._teams:
            return FilterDecision(False)
        if self._channels and not message.is_direct and message.channel not in self._channels:
            return FilterDecision(False)
        if self._users and message.user not in self._users:
            return FilterDecision(False)
        return ALLOW


class DirectMessageFilter:
    """Drops direct messages when they are not accepted."""

    def __init__(self, allow: bool = True) -> None:
        self._allow = allow

    def check(self, message: UserMessage) -> FilterDecision:
        if message.is_direct and not self._allow:
            return FilterDecision(False)
        return ALLOW


class SenderKindFilter:
    """Drops guests / external users. A sender that could not be classified is dropped too
    (only for the kinds that are denied)."""

    def __init__(self, deny_guests: bool = False, deny_external_users: bool = False) -> None:
        self._deny_guests = deny_guests
        self._deny_external = deny_external_users

    def check(self, message: UserMessage) -> FilterDecision:
        sender = message.sender
        if self._deny_guests and (sender is None or sender.is_guest is not False):
            return FilterDecision(False)
        if self._deny_external and (sender is None or sender.is_external is not False):
            return FilterDecision(False)
        return ALLOW


class LengthFilter:
    def __init__(self, max_chars: int = 8000) -> None:
        self._max = max_chars

    def check(self, message: UserMessage) -> FilterDecision:
        if not message.text.strip() and not message.attachments:
            return FilterDecision(False)
        if len(message.text) > self._max:
            return FilterDecision(
                False,
                f"メッセージが長すぎます（上限 {self._max} 文字）。短くして送り直してください。",
            )
        return ALLOW


class RateLimitFilter:
    """At most `per_minute` messages per user per rolling minute."""

    def __init__(self, clock: Clock, per_minute: int = 20) -> None:
        self._clock = clock
        self._limit = per_minute
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def check(self, message: UserMessage) -> FilterDecision:
        if self._limit <= 0:
            return ALLOW
        now = self._clock.now().timestamp()
        hits = self._hits[message.user]
        while hits and now - hits[0] >= 60:
            hits.popleft()
        if len(hits) >= self._limit:
            return FilterDecision(False, "送信が多すぎます。少し待ってから送ってください。")
        hits.append(now)
        return ALLOW


def default_chain(
    clock: Clock,
    *,
    teams: Sequence[str] = (),
    channels: Sequence[str] = (),
    users: Sequence[str] = (),
    allow_direct_messages: bool = True,
    deny_guests: bool = False,
    deny_external_users: bool = False,
    max_chars: int = 8000,
    per_minute: int = 20,
) -> FilterChain:
    return FilterChain(
        [
            SenderFilter(),
            AllowListFilter(teams, channels, users),
            DirectMessageFilter(allow_direct_messages),
            SenderKindFilter(deny_guests, deny_external_users),
            LengthFilter(max_chars),
            RateLimitFilter(clock, per_minute),
        ]
    )
