"""Slack user lookup (`users.info`), cached per user."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from silk_agent.core.clock import SystemClock
from silk_agent.core.ports import Clock

log = logging.getLogger(__name__)

CACHE_SECONDS = 600


@dataclass(frozen=True)
class SlackUser:
    email: str = ""
    is_guest: bool = False
    team_id: str = ""
    enterprise_id: str = ""


class UserDirectory:
    """Resolves a Slack user id to e-mail address, guest flag and home workspace
    (needs `users:read` and `users:read.email`). Results are cached for 10 minutes;
    a failed lookup is not cached."""

    def __init__(self, client: Any, clock: Clock | None = None) -> None:
        self._client = client
        self._clock = clock or SystemClock()
        self._cache: dict[str, tuple[datetime, SlackUser]] = {}

    async def lookup(self, user_id: str) -> SlackUser | None:
        """`None` when Slack could not tell."""
        now = self._clock.now()
        cached = self._cache.get(user_id)
        if cached is not None and now < cached[0]:
            return cached[1]
        try:
            res = await self._client.users_info(user=user_id)
            user = res["user"]
            info = SlackUser(
                email=(user.get("profile") or {}).get("email", "") or "",
                is_guest=bool(user.get("is_restricted") or user.get("is_ultra_restricted")),
                team_id=user.get("team_id", "") or "",
                enterprise_id=(user.get("enterprise_user") or {}).get("enterprise_id", "") or "",
            )
        except Exception:
            log.warning("could not look up the Slack user %s", user_id)
            return None
        self._cache[user_id] = (now + timedelta(seconds=CACHE_SECONDS), info)
        return info

    async def email(self, user_id: str) -> str:
        info = await self.lookup(user_id)
        return info.email if info is not None else ""
