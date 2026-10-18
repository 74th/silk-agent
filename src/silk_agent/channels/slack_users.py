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
# Slack answers about THIS user that will not change by waiting: the user is gone or invisible to
# the app. Anything else is not the user's doing: rate limit, network and 5xx are transient, and
# ``missing_scope`` / ``account_inactive`` are a token or app problem an operator fixes, after
# which the waiting messages must still be processed.
DEFINITIVE_ERRORS = frozenset({"user_not_found", "user_not_visible"})
MISCONFIGURED_ERRORS = frozenset({"missing_scope", "account_inactive", "invalid_auth"})


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
        return (await self.lookup_checked(user_id))[0]

    async def lookup_checked(self, user_id: str) -> tuple[SlackUser | None, bool]:
        """Like ``lookup`` but also says whether a missing result is definitive.

        ``(None, True)``: Slack answered that this user cannot be resolved. ``(None, False)``:
        the lookup failed for a reason that can pass (rate limit, outage). A caller that gives up
        must only do so for the first kind, or a Slack outage would discard every message.
        """
        now = self._clock.now()
        cached = self._cache.get(user_id)
        if cached is not None and now < cached[0]:
            return cached[1], False
        try:
            res = await self._client.users_info(user=user_id)
            user = res["user"]
            info = SlackUser(
                email=(user.get("profile") or {}).get("email", "") or "",
                is_guest=bool(user.get("is_restricted") or user.get("is_ultra_restricted")),
                team_id=user.get("team_id", "") or "",
                enterprise_id=(user.get("enterprise_user") or {}).get("enterprise_id", "") or "",
            )
        except Exception as exc:
            code = _slack_error_code(exc)
            if code in MISCONFIGURED_ERRORS:
                log.error(
                    "Slack user lookup is misconfigured (%s): check the app token and its "
                    "users:read / users:read.email scopes; messages wait until it is fixed",
                    code,
                )
            log.warning("could not look up the Slack user %s (%s)", user_id, code or "transient")
            return None, code in DEFINITIVE_ERRORS
        self._cache[user_id] = (now + timedelta(seconds=CACHE_SECONDS), info)
        return info, False

    async def email(self, user_id: str) -> str:
        info = await self.lookup(user_id)
        return info.email if info is not None else ""


def _slack_error_code(exc: Exception) -> str:
    """The ``error`` field of a Slack API error response, or ``""`` (not a Slack answer)."""
    response = getattr(exc, "response", None)
    try:
        code = response.get("error") if response is not None else ""
    except Exception:
        return ""
    return code if isinstance(code, str) else ""
