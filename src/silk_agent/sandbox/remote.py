"""HTTP client for the authenticated Sandbox service session API."""

from __future__ import annotations

import asyncio
import base64
import uuid
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any

import aiohttp

from silk_agent.core.ports import CommandResult, FileEntry
from silk_agent.sandbox.output_archive import OutputPage


class SandboxServiceError(RuntimeError):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


class RemoteSandboxSessions:
    def __init__(
        self,
        base_url: str,
        *,
        token_file: str | Path = "/var/run/secrets/tokens/silk-sandbox",
        # A cold Sandbox start can take over a minute inside ensure()/the first command.
        timeout_seconds: float = 180,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token_file = Path(token_file)
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def ensure(self, conversation_key: str) -> dict[str, Any]:
        return await self._request(
            "POST", "/v1/sessions/ensure", json={"conversation_key": conversation_key}
        )

    async def status(self, conversation_key: str) -> dict[str, Any]:
        return await self._request(
            "GET", "/v1/sessions/status", params={"conversation_key": conversation_key}
        )

    async def is_ready(self, conversation_key: str) -> bool:
        """Read persisted status only; this endpoint never starts or touches a Sandbox."""
        state = await self.status(conversation_key)
        return state.get("ready") is True

    async def notices(self, conversation_key: str) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/v1/sessions/notices", params={"conversation_key": conversation_key}
        )
        notices = response.get("notices")
        if not isinstance(notices, list):
            raise SandboxServiceError(502, "invalid_response", "service returned invalid notices")
        return notices

    async def acknowledge_notice(self, conversation_key: str, event_id: str) -> bool:
        response = await self._request(
            "POST",
            "/v1/sessions/notices/ack",
            json={"conversation_key": conversation_key, "event_id": event_id},
        )
        return response.get("acknowledged") is True

    async def read_file(
        self,
        conversation_key: str,
        path: str,
        *,
        owner_epoch: int,
        conversation_generation: int,
        turn_id: str,
        origin_seq: int,
        request_id: str | None = None,
    ) -> bytes:
        response = await self._request(
            "POST",
            "/v1/files/read",
            json={
                **_identity(
                    conversation_key,
                    owner_epoch,
                    conversation_generation,
                    turn_id,
                    origin_seq,
                    request_id,
                ),
                "path": path,
            },
        )
        return _decode_response(response.get("data_base64"))

    async def write_file(
        self,
        conversation_key: str,
        path: str,
        content: bytes,
        *,
        owner_epoch: int,
        conversation_generation: int,
        turn_id: str,
        origin_seq: int,
        request_id: str | None = None,
    ) -> str:
        response = await self._request(
            "PUT",
            "/v1/files/write",
            json={
                **_identity(
                    conversation_key,
                    owner_epoch,
                    conversation_generation,
                    turn_id,
                    origin_seq,
                    request_id,
                ),
                "path": path,
                "data_base64": base64.b64encode(content).decode("ascii"),
            },
        )
        return str(response.get("state", "unknown"))

    async def list_files(
        self,
        conversation_key: str,
        path: str = ".",
        *,
        owner_epoch: int,
        conversation_generation: int,
        turn_id: str,
        origin_seq: int,
        request_id: str | None = None,
    ) -> list[FileEntry]:
        response = await self._request(
            "POST",
            "/v1/files/list",
            json={
                **_identity(
                    conversation_key,
                    owner_epoch,
                    conversation_generation,
                    turn_id,
                    origin_seq,
                    request_id,
                ),
                "path": path,
            },
        )
        entries = response.get("entries")
        if not isinstance(entries, list):
            raise SandboxServiceError(502, "invalid_response", "service returned invalid file list")
        return [
            FileEntry(str(item["path"]), bool(item["is_dir"]), int(item["size"]))
            for item in entries
            if isinstance(item, dict)
        ]

    async def sync_skills(
        self,
        conversation_key: str,
        files: Mapping[str, bytes],
        skills_generation: str,
        *,
        owner_epoch: int,
        conversation_generation: int,
        turn_id: str,
        origin_seq: int,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        encoded = {
            path: base64.b64encode(content).decode("ascii") for path, content in files.items()
        }
        return await self._request(
            "PUT",
            "/v1/skills/sync",
            json={
                **_identity(
                    conversation_key,
                    owner_epoch,
                    conversation_generation,
                    turn_id,
                    origin_seq,
                    request_id,
                ),
                "skills_generation": skills_generation,
                "files": encoded,
            },
        )

    async def start_command(
        self,
        conversation_key: str,
        command: str,
        *,
        cwd: str,
        timeout_seconds: int,
        owner_epoch: int,
        conversation_generation: int,
        turn_id: str,
        origin_seq: int,
        request_id: str,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/v1/commands",
            json={
                **_identity(
                    conversation_key,
                    owner_epoch,
                    conversation_generation,
                    turn_id,
                    origin_seq,
                    request_id,
                ),
                "command": command,
                "cwd": cwd,
                "timeout_seconds": timeout_seconds,
            },
        )

    async def command_status(
        self, conversation_key: str, command_id: str, **identity: Any
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/v1/commands/{command_id}/status",
            json={
                **_identity(
                    conversation_key,
                    identity["owner_epoch"],
                    identity["conversation_generation"],
                    identity["turn_id"],
                    identity["origin_seq"],
                    command_id,
                )
            },
        )

    async def command_output(
        self,
        conversation_key: str,
        command_id: str,
        *,
        stream: str,
        cursor: int,
        limit_bytes: int = 65536,
        **identity: Any,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/v1/commands/{command_id}/output",
            json={
                **_identity(
                    conversation_key,
                    identity["owner_epoch"],
                    identity["conversation_generation"],
                    identity["turn_id"],
                    identity["origin_seq"],
                    command_id,
                ),
                "stream": stream,
                "cursor": cursor,
                "limit_bytes": limit_bytes,
            },
        )

    async def cancel_command(
        self,
        conversation_key: str,
        command_id: str,
        *,
        request_id: str | None = None,
        **identity: Any,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/v1/commands/{command_id}/cancel",
            json={
                **_identity(
                    conversation_key,
                    identity["owner_epoch"],
                    identity["conversation_generation"],
                    identity["turn_id"],
                    identity["origin_seq"],
                    request_id or uuid.uuid4().hex,
                ),
                "reason": "user",
            },
        )

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        token = (await asyncio.to_thread(self._token_file.read_text, encoding="utf-8")).strip()
        if not token:
            raise SandboxServiceError(401, "missing_token", "projected service token is empty")
        headers = {"Authorization": f"Bearer {token}"}
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            async with session.request(
                method, f"{self._base_url}{path}", headers=headers, **kwargs
            ) as response:
                try:
                    body = await response.json()
                except (ValueError, aiohttp.ContentTypeError):
                    body = {}
                if response.status >= 400:
                    error = body.get("error", {}) if isinstance(body, dict) else {}
                    raise SandboxServiceError(
                        response.status,
                        str(error.get("code", "service_error")),
                        str(error.get("message", "Sandbox service request failed")),
                    )
                if not isinstance(body, dict):
                    raise SandboxServiceError(
                        502, "invalid_response", "service response is invalid"
                    )
                return body


IdentityProvider = Callable[[str], Awaitable[Mapping[str, Any]]]


class RemoteSandboxRuntime:
    """SandboxRuntime adapter; cancelling a wait never cancels the remote command."""

    def __init__(
        self,
        sessions: RemoteSandboxSessions,
        identity_provider: IdentityProvider,
        *,
        default_timeout_seconds: float = 120,
        poll_interval: float = 0.2,
    ) -> None:
        if default_timeout_seconds <= 0 or poll_interval <= 0:
            raise ValueError("timeouts must be positive")
        self.sessions = sessions
        self._identity_provider = identity_provider
        self._timeout = default_timeout_seconds
        self._poll_interval = poll_interval
        self._running: dict[str, dict[str, Mapping[str, Any]]] = {}
        self._notices: dict[str, list[str]] = {}
        self._notice_ids: dict[str, list[str]] = {}

    async def run_command(
        self,
        conversation_key: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout_seconds: float | None = None,
        on_started: Callable[[str], Awaitable[None]] | None = None,
    ) -> CommandResult:
        command_id = uuid.uuid4().hex
        identity = await self._identity(conversation_key)
        # A cold start of the Sandbox can take as long as this client waits. Do it in its own
        # request, before the command exists: if it times out nothing has run, whereas a start
        # that times out while the service goes on would run the command that a retry then
        # runs again.
        await self.sessions.ensure(conversation_key)
        response = await self.sessions.start_command(
            conversation_key,
            command,
            cwd=cwd or "",
            timeout_seconds=max(1, int(timeout_seconds or self._timeout)),
            request_id=command_id,
            **identity,
        )
        self._running.setdefault(conversation_key, {})[command_id] = identity
        if on_started is not None:
            await on_started(command_id)
        if response.get("state") == "unknown":
            return CommandResult(command_id, "unknown")

        wait_cancelled = False
        try:
            while True:
                status = await self.sessions.command_status(
                    conversation_key, command_id, **identity
                )
                state = status.get("state")
                if state == "unknown":
                    return CommandResult(command_id, "unknown")
                if state in {"finished", "cancelled"}:
                    stdout = await self._read_all_output(
                        conversation_key, command_id, identity, "stdout"
                    )
                    stderr = await self._read_all_output(
                        conversation_key, command_id, identity, "stderr"
                    )
                    return CommandResult(
                        command_id,
                        "cancelled" if state == "cancelled" else "completed",
                        status.get("exit_code"),
                        stdout.decode("utf-8", errors="replace"),
                        stderr.decode("utf-8", errors="replace"),
                    )
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            wait_cancelled = True
            raise
        finally:
            if not wait_cancelled:
                self._running.get(conversation_key, {}).pop(command_id, None)

    async def command_status(self, conversation_key: str, command_id: str) -> CommandResult:
        identity = self._running.get(conversation_key, {}).get(command_id)
        if identity is None:
            identity = await self._identity(conversation_key)
        record = await self.sessions.command_status(conversation_key, command_id, **identity)
        state = record.get("state")
        outcome = (
            "running"
            if state == "running"
            else "cancelled"
            if state == "cancelled"
            else "completed"
            if state == "finished"
            else "unknown"
        )
        return CommandResult(command_id, outcome, record.get("exit_code"))

    async def command_output(
        self, conversation_key: str, command_id: str, cursor: int = 0
    ) -> tuple[str, int]:
        identity = self._running.get(conversation_key, {}).get(command_id)
        if identity is None:
            identity = await self._identity(conversation_key)
        page = await self.sessions.command_output(
            conversation_key,
            command_id,
            stream="stdout",
            cursor=cursor,
            **identity,
        )
        return _decode_response(page.get("data_base64")).decode("utf-8", errors="replace"), int(
            page["next_cursor"]
        )

    async def read_output_page(
        self,
        conversation_key: str,
        command_id: str,
        *,
        stream: str,
        cursor: int,
        limit_bytes: int,
    ) -> OutputPage:
        identity = self._running.get(conversation_key, {}).get(command_id)
        if identity is None:
            identity = await self._identity(conversation_key)
        page = await self.sessions.command_output(
            conversation_key,
            command_id,
            stream=stream,
            cursor=cursor,
            limit_bytes=limit_bytes,
            **identity,
        )
        try:
            next_cursor = int(page["next_cursor"])
            complete = page["complete"]
            data = _decode_response(page.get("data_base64"))
        except (KeyError, TypeError, ValueError) as exc:
            raise SandboxServiceError(
                502, "invalid_response", "service returned invalid output"
            ) from exc
        if not isinstance(complete, bool):
            raise SandboxServiceError(502, "invalid_response", "service returned invalid output")
        return OutputPage(data, next_cursor, complete, page.get("truncated") is True)

    async def cancel_commands(self, conversation_key: str) -> None:
        for command_id, identity in list(self._running.get(conversation_key, {}).items()):
            await self.sessions.cancel_command(conversation_key, command_id, **identity)
            self._running.get(conversation_key, {}).pop(command_id, None)

    async def cancel_command(self, conversation_key: str, command_id: str, request_id: str) -> str:
        identity = self._running.get(conversation_key, {}).get(command_id)
        if identity is None:
            identity = await self._identity(conversation_key)
        try:
            response = await self.sessions.cancel_command(
                conversation_key, command_id, request_id=request_id, **identity
            )
        except Exception:
            return "unknown"
        state = response.get("state")
        if state == "cancelled":
            return "confirmed"
        if state in {"cancel_requested", "accepted"}:
            return "accepted"
        return "unknown"

    async def read_file(self, conversation_key: str, path: str) -> bytes:
        identity = await self._identity(conversation_key)
        return await self.sessions.read_file(conversation_key, path, **identity)

    async def write_file(self, conversation_key: str, path: str, content: bytes) -> None:
        identity = await self._identity(conversation_key)
        await self.sessions.write_file(conversation_key, path, content, **identity)

    async def list_files(self, conversation_key: str, path: str) -> list[FileEntry]:
        identity = await self._identity(conversation_key)
        return await self.sessions.list_files(conversation_key, path, **identity)

    async def is_ready(self, conversation_key: str) -> bool:
        return await self.sessions.is_ready(conversation_key)

    async def pop_notices(self, conversation_key: str) -> list[str]:
        await self.sessions.ensure(conversation_key)
        notices = await self.sessions.notices(conversation_key)
        self._notices[conversation_key] = [str(item["context_message"]) for item in notices]
        self._notice_ids[conversation_key] = [str(item["event_id"]) for item in notices]
        return list(self._notices[conversation_key])

    async def ack_notices(self, conversation_key: str) -> None:
        for event_id in self._notice_ids.get(conversation_key, []):
            await self.sessions.acknowledge_notice(conversation_key, event_id)
        self._notice_ids.pop(conversation_key, None)
        self._notices.pop(conversation_key, None)

    async def copy_skills(self, conversation_key: str, source: Path, generation: str) -> None:
        skills = source / ".claude" / "skills"
        if not skills.is_dir():
            return
        files = {}
        for path in sorted(item for item in skills.rglob("*") if item.is_file()):
            files[path.relative_to(skills).as_posix()] = await asyncio.to_thread(path.read_bytes)
        identity = await self._identity(conversation_key)
        result = await self.sessions.sync_skills(conversation_key, files, generation, **identity)
        if result.get("complete") is not True:
            raise SandboxServiceError(
                502,
                "skills_sync_incomplete",
                f"Skills sync failed for {len(result.get('failed_paths', []))} file(s)",
            )

    async def _identity(self, conversation_key: str) -> dict[str, Any]:
        identity = dict(await self._identity_provider(conversation_key))
        required = {"owner_epoch", "conversation_generation", "turn_id", "origin_seq"}
        if not required.issubset(identity):
            raise ValueError("identity provider returned incomplete owner context")
        return {name: identity[name] for name in required}

    async def _read_all_output(
        self,
        conversation_key: str,
        command_id: str,
        identity: Mapping[str, Any],
        stream: str,
    ) -> bytes:
        chunks = bytearray()
        cursor = 0
        while True:
            page = await self.sessions.command_output(
                conversation_key,
                command_id,
                stream=stream,
                cursor=cursor,
                **identity,
            )
            chunks.extend(_decode_response(page.get("data_base64")))
            next_cursor = int(page["next_cursor"])
            if page.get("complete") is True or next_cursor == cursor:
                return bytes(chunks)
            cursor = next_cursor


def _identity(
    conversation_key: str,
    owner_epoch: int,
    conversation_generation: int,
    turn_id: str,
    origin_seq: int,
    request_id: str | None,
) -> dict[str, Any]:
    return {
        "request_id": request_id or uuid.uuid4().hex,
        "conversation_key": conversation_key,
        "owner_epoch": owner_epoch,
        "conversation_generation": conversation_generation,
        "turn_id": turn_id,
        "origin_seq": origin_seq,
    }


def _decode_response(value: Any) -> bytes:
    if not isinstance(value, str):
        raise SandboxServiceError(502, "invalid_response", "service returned invalid file content")
    try:
        return base64.b64decode(value, validate=True)
    except ValueError as exc:
        raise SandboxServiceError(
            502, "invalid_response", "service returned invalid file content"
        ) from exc
