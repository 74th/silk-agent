"""GkeSessionSandboxRuntime: SandboxRuntime on the `gke-session-sandbox` library.

Uses `manager.ensure`, `session.commands` (start / wait / get / read_output / cancel),
`session.files`, `session.pending_context_events` and `manager.ack_context_event`.
The library is imported lazily, so the rest of silk-agent works without it.
"""

from __future__ import annotations

import asyncio
import logging
import math
import uuid
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
        cancel_confirm_seconds: float = 10.0,
    ) -> None:
        self._manager = manager
        self._env_provider = env_provider
        self._key_factory = session_key_factory
        self._timeout = default_timeout_seconds
        # The tracker sends SIGTERM, and SIGKILL 5 s later: wait for the exit past that.
        self._cancel_confirm = cancel_confirm_seconds
        self._handles: dict[str, Any] = {}
        self._pending: dict[str, list[str]] = {}
        self._events: dict[str, list[Any]] = {}
        self._running: dict[str, dict[str, Any]] = {}  # conversation -> command id -> handle
        self._cancelled: dict[str, set[str]] = {}

    # ------------------------------------------------------------ session

    def _session_key(self, key: str) -> Any:
        tenant, session = to_session_key_parts(key)
        if self._key_factory is not None:
            return self._key_factory(tenant, session)
        from gke_session_sandbox import SessionKey

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
        """Submit once with our own command ID, then wait for it by ID.

        Only the submission may be retried (on a new generation); waiting never resends.
        """
        timeout = max(1, math.ceil(timeout_seconds or self._timeout))
        command_id = uuid.uuid4().hex

        async def submit(handle: Any) -> Any:
            await handle.commands.start(
                ["bash", "-lc", command],
                command_id=command_id,
                cwd=cwd or "",
                timeout_seconds=timeout,
            )
            return handle

        try:
            handle = await self._with_session(conversation_key, submit)
        except Exception as e:
            if type(e).__name__ == "ExecutionOutcomeUnknown":
                if on_started is not None:
                    await on_started(command_id)
                return CommandResult(command_id, "unknown")
            raise
        running = self._running.setdefault(conversation_key, {})
        running[command_id] = handle
        wait_cancelled = False
        try:
            if on_started is not None:
                await on_started(command_id)
            result = await handle.commands.wait(command_id)
        except asyncio.CancelledError:
            # Preserve the handle so a separate explicit cancel can still target it.
            wait_cancelled = True
            raise
        except Exception as e:
            if type(e).__name__ == "ExecutionOutcomeUnknown":
                return CommandResult(command_id, "unknown")
            raise
        finally:
            if not wait_cancelled:
                running.pop(command_id, None)
        cancelled = command_id in self._cancelled.pop(conversation_key, set()) or (
            getattr(result, "stop_reason", None) == "cancelled"
        )
        return CommandResult(
            command_id,
            "cancelled" if cancelled else "completed",
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
        state = str(record.get("state", "unknown"))
        if state == "finished":
            outcome = "cancelled" if record.get("stop_reason") == "cancelled" else "completed"
        else:
            outcome = "running" if state == "running" else "unknown"
        return CommandResult(command_id, outcome, record.get("exit_code"))  # type: ignore[arg-type]

    async def command_output(
        self, conversation_key: str, command_id: str, cursor: int = 0
    ) -> tuple[str, int]:
        """stdout from a byte cursor (the cursor returned by the previous call)."""

        async def op(handle: Any) -> Any:
            return await handle.commands.read_output(command_id, stream="stdout", cursor=cursor)

        page = await self._with_session(conversation_key, op)
        return _text(page.data), int(page.cursor)

    async def cancel_command(
        self, conversation_key: str, command_id: str, request_id: str | None = None
    ) -> str:
        """Ask the tracker to stop one command.

        "confirmed" only once the tracker reports it finished; "accepted" when the cancel
        was taken but the process is still running (the tracker's cancel returns at once, with
        SIGKILL following later); "unknown" when it could not be sent or read. Only a
        confirmed stop drops the local handle."""
        handle = self._running.get(conversation_key, {}).get(command_id)
        self._cancelled.setdefault(conversation_key, set()).add(command_id)

        async def ask(call: Callable[[Any], Awaitable[Any]]) -> Any:
            if handle is not None:
                return await call(handle)
            return await self._with_session(conversation_key, call)

        try:
            await ask(lambda session: session.commands.cancel(command_id))
        except Exception:
            log.exception("cancelling command %s failed", command_id)
            self._cancelled.get(conversation_key, set()).discard(command_id)
            return "unknown"
        deadline = asyncio.get_running_loop().time() + self._cancel_confirm
        while True:
            try:
                record = await ask(lambda session: session.commands.get(command_id))
            except Exception:
                log.exception("reading command %s after cancel failed", command_id)
                return "unknown"
            if record.get("state") == "finished":
                self._running.get(conversation_key, {}).pop(command_id, None)
                return "confirmed"
            if asyncio.get_running_loop().time() >= deadline:
                return "accepted"
            await asyncio.sleep(0.25)

    async def cancel_commands(self, conversation_key: str) -> None:
        """Ask the tracker to stop the running commands (SIGTERM, then SIGKILL)."""
        for command_id, handle in list(self._running.get(conversation_key, {}).items()):
            self._cancelled.setdefault(conversation_key, set()).add(command_id)
            try:
                await handle.commands.cancel(command_id)
            except Exception:
                log.exception("cancelling command %s failed", command_id)
            else:
                self._running.get(conversation_key, {}).pop(command_id, None)

    # ------------------------------------------------------------ files

    async def read_file(self, conversation_key: str, path: str) -> bytes:
        async def op(handle: Any) -> bytes:
            return await handle.files.read(path)

        try:
            return await self._with_session(conversation_key, op)
        except Exception as e:
            if type(e).__name__ in ("FileNotFoundError", "NotFound") or (
                type(e).__name__ == "FileOperationError" and getattr(e, "status", None) == 404
            ):
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
                getattr(e, "type", "") == "directory",
                int(getattr(e, "size", 0) or 0),
            )
            for e in entries
        ]

    # ------------------------------------------------------------ notices / skills

    async def is_ready(self, conversation_key: str) -> bool:
        """Read-only: does not start the sandbox nor move its idle clock."""
        try:
            handle = await self._manager.get(self._session_key(conversation_key))
        except Exception:
            log.exception("could not read the sandbox state of %s", conversation_key)
            return True  # unknown: do not announce a start-up
        return handle is not None and str(getattr(handle, "state", "")) == "ready"

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

    @property
    def manager(self) -> Any:
        return self._manager

    async def aclose(self) -> None:
        close = getattr(self._manager, "close", None)
        if close is not None:
            await close()


def _text(value: Any) -> str:
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value or "")
