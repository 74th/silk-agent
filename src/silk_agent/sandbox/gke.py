"""GkeSessionSandboxRuntime: SandboxRuntime on the `gke-session-sandbox` library.

Written against the API documented in the library's README (`manager.ensure`,
`session.commands.run`, `session.files.*`, `session.pending_context_events`,
`manager.ack_context_event`). The library is imported lazily, so the rest of silk-agent
works without it.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any

from silk_agent.core.ports import CommandResult, FileEntry

log = logging.getLogger(__name__)

SKILLS_DIR = ".skills"
EnvProvider = Callable[[str], Awaitable[Mapping[str, str]]]


def to_session_key_parts(conversation_key: str) -> tuple[str, str]:
    """`slack:T:C:ts` -> ("T", "C:ts"), `streamlit:abc` -> ("streamlit", "abc")."""
    scheme, _, rest = conversation_key.partition(":")
    if scheme == "slack":
        team, _, remainder = rest.partition(":")
        return team or "slack", remainder or "-"
    return scheme, rest or "-"


class GkeSessionSandboxRuntime:
    def __init__(
        self,
        manager: Any,
        *,
        env_provider: EnvProvider | None = None,
        session_key_factory: Callable[[str, str], Any] | None = None,
        default_timeout_seconds: float = 120.0,
    ) -> None:
        self._manager = manager
        self._env_provider = env_provider
        self._key_factory = session_key_factory
        self._timeout = default_timeout_seconds
        self._handles: dict[str, Any] = {}
        self._pending: dict[str, list[str]] = {}
        self._events: dict[str, list[Any]] = {}
        self._running: dict[str, set[asyncio.Future[Any]]] = {}
        self._fallback_ids = itertools.count(1)

    # ------------------------------------------------------------ session

    def _session_key(self, key: str) -> Any:
        tenant, session = to_session_key_parts(key)
        if self._key_factory is not None:
            return self._key_factory(tenant, session)
        from gke_session_sandbox import SessionKey  # pyright: ignore[reportMissingImports]

        return SessionKey(tenant_id=tenant, session_id=session)

    async def _ensure(self, key: str, *, fresh: bool = False) -> Any:
        """The Ready session handle for the conversation (creates / resumes as needed)."""
        if not fresh and key in self._handles:
            return self._handles[key]
        env = dict(await self._env_provider(key)) if self._env_provider else {}
        handle = await self._manager.ensure(self._session_key(key), env=env or None)
        self._handles[key] = handle
        self._pending[key] = [e.event_id for e in getattr(handle, "pending_context_events", ())]
        self._events[key] = list(getattr(handle, "pending_context_events", ()))
        return handle

    async def _with_session(self, key: str, op: Callable[[Any], Awaitable[Any]]) -> Any:
        handle = await self._ensure(key)
        try:
            return await op(handle)
        except Exception as e:
            if type(e).__name__ != "StaleGenerationError":
                raise
        # The sandbox generation ended: ensure a new one (notices explain what was lost).
        handle = await self._ensure(key, fresh=True)
        return await op(handle)

    # ------------------------------------------------------------ commands

    async def run_command(
        self,
        conversation_key: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout_seconds: float | None = None,
        on_started: Callable[[str], Awaitable[None]] | None = None,
    ) -> CommandResult:
        timeout = timeout_seconds or self._timeout

        async def op(handle: Any) -> Any:
            inner = asyncio.ensure_future(
                handle.commands.run(["bash", "-lc", command], cwd=cwd or ".", timeout=timeout)
            )
            self._running.setdefault(conversation_key, set()).add(inner)
            try:
                return await inner
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if inner.cancelled() and not (current and current.cancelling()):
                    return None  # cancelled by cancel_commands()
                raise
            finally:
                self._running[conversation_key].discard(inner)

        try:
            # A command is sent at most once: retrying is limited to ensuring the session.
            result = await self._with_session(conversation_key, op)
        except Exception as e:
            if type(e).__name__ == "ExecutionOutcomeUnknown":
                command_id = getattr(e, "command_id", "") or f"unknown-{next(self._fallback_ids)}"
                if on_started is not None:
                    await on_started(command_id)
                return CommandResult(command_id, "unknown")
            raise
        if result is None:
            return CommandResult("", "cancelled")
        command_id = str(getattr(result, "command_id", "") or "")
        if on_started is not None and command_id:
            await on_started(command_id)
        return CommandResult(
            command_id,
            "completed",
            getattr(result, "exit_code", None),
            _text(getattr(result, "stdout", "")),
            _text(getattr(result, "stderr", "")),
        )

    async def command_status(self, conversation_key: str, command_id: str) -> CommandResult:
        async def op(handle: Any) -> Any:
            return await handle.commands.get(command_id)

        try:
            record = await self._with_session(conversation_key, op)
        except Exception:
            log.exception("command status lookup failed")
            return CommandResult(command_id, "unknown")
        state = str(getattr(record, "state", "unknown")).lower()
        outcome = {
            "running": "running",
            "completed": "completed",
            "succeeded": "completed",
            "failed": "completed",
            "cancelled": "cancelled",
        }.get(state.rsplit(".", 1)[-1], "unknown")
        return CommandResult(command_id, outcome, getattr(record, "exit_code", None))  # type: ignore[arg-type]

    async def command_output(
        self, conversation_key: str, command_id: str, cursor: int = 0
    ) -> tuple[str, int]:
        async def op(handle: Any) -> Any:
            return await handle.commands.read_output(command_id, cursor=cursor or None)

        out = await self._with_session(conversation_key, op)
        text = _text(getattr(out, "text", None) or getattr(out, "data", ""))
        return text, int(getattr(out, "cursor", cursor + len(text)))

    async def cancel_commands(self, conversation_key: str) -> None:
        """Cancelling a `commands.run` makes the library stop the process (SIGTERM, then
        SIGKILL after 5s) through the tracker."""
        for task in list(self._running.get(conversation_key, ())):
            task.cancel()

    # ------------------------------------------------------------ files

    async def read_file(self, conversation_key: str, path: str) -> bytes:
        async def op(handle: Any) -> bytes:
            return await handle.files.read(path)

        try:
            return await self._with_session(conversation_key, op)
        except Exception as e:
            if type(e).__name__ in ("FileNotFoundError", "NotFound"):
                raise FileNotFoundError(path) from e
            raise

    async def write_file(self, conversation_key: str, path: str, content: bytes) -> None:
        async def op(handle: Any) -> None:
            await handle.files.write(path, content)

        await self._with_session(conversation_key, op)

    async def list_files(self, conversation_key: str, path: str) -> list[FileEntry]:
        async def op(handle: Any) -> Any:
            return await handle.files.list(path)

        entries = await self._with_session(conversation_key, op)
        return [
            FileEntry(
                str(getattr(e, "path", getattr(e, "name", e))),
                bool(getattr(e, "is_dir", False)),
                int(getattr(e, "size", 0) or 0),
            )
            for e in entries
        ]

    # ------------------------------------------------------------ notices / skills

    async def pop_notices(self, conversation_key: str) -> list[str]:
        """Notices from the library (e.g. workspace deleted). They stay pending until acked."""
        await self._ensure(conversation_key, fresh=True)
        return [e.context_message for e in self._events.get(conversation_key, [])]

    async def ack_notices(self, conversation_key: str) -> None:
        """Mark the notices as stored in the conversation (idempotent in the library)."""
        handle = self._handles.get(conversation_key)
        if handle is None:
            return
        for event_id in self._pending.pop(conversation_key, []):
            await self._manager.ack_context_event(handle.key, event_id)
        self._events[conversation_key] = []

    async def copy_skills(self, conversation_key: str, source: Path, generation: str) -> None:
        """Copy `source/.claude/skills` to `/workspace/.skills/` in the sandbox."""
        skills = source / ".claude" / "skills"
        if not skills.is_dir():
            return

        async def op(handle: Any) -> None:
            for file in sorted(p for p in skills.rglob("*") if p.is_file()):
                target = f"{SKILLS_DIR}/{file.relative_to(skills).as_posix()}"
                await handle.files.write(target, await asyncio.to_thread(file.read_bytes))

        await self._with_session(conversation_key, op)

    async def aclose(self) -> None:
        close = getattr(self._manager, "close", None)
        if close is not None:
            await close()


def _text(value: Any) -> str:
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value or "")
