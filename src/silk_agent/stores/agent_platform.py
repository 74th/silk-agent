"""Claude Agent SDK `SessionStore` backed by Agent Platform Sessions.

One Claude session = one Agent Platform session (`c-<uuid>`). Each transcript entry is
stored as one event whose `raw_event` carries `{uuid, subpath, seq, entry}`:

* `load` reads every event, filters by subpath, drops duplicate uuids and sorts by `seq`.
* `seq` is a nanosecond clock that never goes backwards inside the process, so entries keep
  their append order even across Bot restarts.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import logging
import re
import time
import uuid
from collections.abc import Callable
from typing import Any

log = logging.getLogger(__name__)

AUTHOR = "claude-agent-sdk"
USER_ID = "silk-agent"
DEFAULT_TTL_SECONDS = 30 * 24 * 3600

_VALID_ID = re.compile(r"^[a-z][a-z0-9-]{0,61}[a-z0-9]$")


def to_platform_session_id(claude_session_id: str) -> str:
    """UUID -> `c-<uuid>`: lower-case letters, digits and hyphens, at most 63 characters."""
    candidate = "c-" + re.sub(r"[^a-z0-9-]", "-", claude_session_id.lower())
    if _VALID_ID.match(candidate) and len(candidate) <= 63:
        return candidate
    return "c-h" + hashlib.sha256(claude_session_id.encode()).hexdigest()[:40]


def control_session_id(conversation_key: str) -> str:
    return "m-" + hashlib.sha256(conversation_key.encode()).hexdigest()[:40]


def is_already_exists(error: BaseException) -> bool:
    text = f"{type(error).__name__} {error}".lower()
    return "already exists" in text or "already_exists" in text or "409" in text


def is_not_found(error: BaseException) -> bool:
    text = f"{type(error).__name__} {error}".lower()
    return "not found" in text or "not_found" in text or "404" in text or "notfound" in text


def make_client(project: str, location: str) -> Any:
    import vertexai

    client_class: Any = vertexai.Client
    return client_class(project=project, location=location)


class AgentPlatformSessions:
    """Thin async wrapper over the (synchronous) sessions / events APIs."""

    def __init__(
        self,
        agent_runtime: str,
        *,
        project: str = "",
        location: str = "",
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        sessions_api: Any | None = None,
        events_api: Any | None = None,
    ) -> None:
        self.agent_runtime = agent_runtime
        self.ttl_seconds = ttl_seconds
        if sessions_api is None or events_api is None:
            client: Any = make_client(project, location)
            sessions_api = client.agent_engines.sessions
            events_api = client.agent_engines.sessions.events
        self._sessions: Any = sessions_api
        self._events: Any = events_api
        self._known: set[str] = set()
        self._clock_ns = 0

    def session_name(self, session_id: str) -> str:
        return f"{self.agent_runtime}/sessions/{session_id}"

    def next_seq(self) -> int:
        self._clock_ns = max(self._clock_ns + 1, time.time_ns())
        return self._clock_ns

    async def ensure_session(
        self, session_id: str, user_id: str = USER_ID, state: dict[str, Any] | None = None
    ) -> None:
        if session_id in self._known:
            return
        config: dict[str, Any] = {
            "session_id": session_id,
            "wait_for_completion": True,
            "ttl": f"{self.ttl_seconds}s",
        }
        if state:
            config["session_state"] = state
        try:
            await asyncio.to_thread(
                self._sessions.create,
                name=self.agent_runtime,
                user_id=user_id,
                config=config,
            )
        except Exception as e:
            if not is_already_exists(e):
                raise
        self._known.add(session_id)

    async def append_event(
        self, session_id: str, raw_event: dict[str, Any], invocation_id: str | None = None
    ) -> None:
        await asyncio.to_thread(
            self._events.append,
            name=self.session_name(session_id),
            author=AUTHOR,
            invocation_id=invocation_id or f"inv-{uuid.uuid4().hex}",
            timestamp=dt.datetime.now(dt.UTC),
            config={"raw_event": raw_event},
        )

    async def list_raw_events(self, session_id: str) -> list[dict[str, Any]] | None:
        """All events' raw_event dicts, or None if the session does not exist."""

        def read() -> list[dict[str, Any]] | None:
            try:
                events = list(self._events.list(name=self.session_name(session_id)))
            except Exception as e:
                if is_not_found(e):
                    return None
                raise
            out: list[dict[str, Any]] = []
            for event in events:
                raw = event.get("raw_event") if isinstance(event, dict) else event.raw_event
                if raw:
                    out.append(dict(raw))
            return out

        return await asyncio.to_thread(read)

    async def list_sessions(self, user_id: str) -> list[str]:
        """Session ids (the part after `/sessions/`) of the given user."""

        def read() -> list[str]:
            sessions = self._sessions.list(
                name=self.agent_runtime, config={"filter": f'user_id="{user_id}"'}
            )
            return [
                (s["name"] if isinstance(s, dict) else s.name).rsplit("/", 1)[-1] for s in sessions
            ]

        return await asyncio.to_thread(read)


class AgentPlatformSessionStore:
    """Implements the SDK's `SessionStore` protocol (`append` / `load`)."""

    def __init__(
        self,
        sessions: AgentPlatformSessions,
        *,
        invocation_id: Callable[[], str | None] | None = None,
    ) -> None:
        self._api = sessions
        self._invocation_id = invocation_id

    async def append(self, key: dict[str, Any], entries: list[dict[str, Any]]) -> None:
        if not entries:
            return
        session_id = to_platform_session_id(key["session_id"])
        await self._api.ensure_session(session_id)
        invocation = self._invocation_id() if self._invocation_id else None
        for entry in entries:
            await self._api.append_event(
                session_id,
                {
                    "uuid": entry.get("uuid"),
                    "project_key": key.get("project_key", ""),
                    "subpath": key.get("subpath", ""),
                    "seq": self._api.next_seq(),
                    "entry": entry,
                },
                invocation,
            )

    async def load(self, key: dict[str, Any]) -> list[dict[str, Any]] | None:
        session_id = to_platform_session_id(key["session_id"])
        raw_events = await self._api.list_raw_events(session_id)
        if raw_events is None:
            return None
        subpath = key.get("subpath", "")
        project_key = key.get("project_key", "")
        selected = [
            r
            for r in raw_events
            if r.get("subpath", "") == subpath and r.get("project_key", "") == project_key
        ]
        selected.sort(key=lambda r: r.get("seq", 0))
        seen: set[str] = set()
        entries: list[dict[str, Any]] = []
        for r in selected:
            u = r.get("uuid")
            if u is not None:
                if u in seen:
                    continue
                seen.add(u)
            entries.append(r["entry"])
        return entries or None
