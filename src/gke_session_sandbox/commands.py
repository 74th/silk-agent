"""Reconnectable HTTP client for the in-Pod Python CommandTracker."""

from __future__ import annotations

import asyncio
import base64
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass

import aiohttp

from .errors import (
    CommandFailedError, CommandOutputLimitExceeded, ExecutionOutcomeUnknown,
    OutputExpiredError, OutputIncompleteError, OutputStorageLimitExceeded, RuntimeConnectionError,
)

EndpointResolver = Callable[[], Awaitable[str]]
BeforeCommandSubmit = Callable[[str], Awaitable[None]]
CommandObserver = Callable[[str, Mapping[str, object]], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class CommandResult:
    command_id: str
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    stop_reason: str | None
    output_complete: bool


@dataclass(frozen=True, slots=True)
class OutputPage:
    command_id: str
    stream: str
    data: bytes
    cursor: int
    total_bytes: int
    complete: bool
    output_complete: bool


class CommandTrackerClient:
    """Submit once, then reconnect by command ID without repeating side effects.

    ``endpoint_resolver`` is called for every tracker request. A SessionHandle
    supplies a resolver that ensures/resumes its session before looking up the
    current Service address, so callers do not need a heartbeat or manual
    activity accounting.
    """

    def __init__(self, endpoint_resolver: EndpointResolver, *, poll_interval: float = 0.1,
                 request_timeout: float = 10, buffered_response_limit_bytes: int = 8 * 1024 * 1024,
                 before_submit: BeforeCommandSubmit | None = None,
                 command_observer: CommandObserver | None = None) -> None:
        if poll_interval <= 0 or request_timeout <= 0 or buffered_response_limit_bytes <= 0:
            raise ValueError("poll_interval and request_timeout must be positive")
        self._endpoint_resolver = endpoint_resolver
        self._poll_interval = poll_interval
        self._request_timeout = request_timeout
        self._buffered_response_limit_bytes = buffered_response_limit_bytes
        self._before_submit = before_submit
        self._command_observer = command_observer
        self._session: aiohttp.ClientSession | None = None
        self._endpoint: str | None = None

    async def __aenter__(self) -> "CommandTrackerClient":
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._request_timeout))
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _http(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._request_timeout))
        return self._session

    async def _request(self, command_id: str, method: str, path: str, body=None, *, ambiguous=False):
        try:
            if self._endpoint is None:
                self._endpoint = (await self._endpoint_resolver()).rstrip("/")
            base = self._endpoint
            async with self._http().request(method, base + path, json=body) as response:
                payload = await response.read()
                if response.status >= 400:
                    detail = payload.decode(errors="replace")
                    if response.status == 410:
                        raise OutputExpiredError(command_id)
                    if ambiguous and response.status >= 500:
                        raise ExecutionOutcomeUnknown(command_id, f"tracker returned HTTP {response.status}")
                    raise RuntimeConnectionError(f"tracker returned HTTP {response.status}: {detail}")
                if not payload:
                    return None
                return await _decode_json(payload, command_id)
        except ExecutionOutcomeUnknown:
            raise
        except RuntimeConnectionError:
            # A tracker HTTP rejection is a known outcome (for example, a
            # command ID conflict or a closed admission gate), not a lost
            # submission response.
            self._endpoint = None
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            self._endpoint = None
            if ambiguous:
                raise ExecutionOutcomeUnknown(command_id, "command submission response was lost; it was not retried") from exc
            if isinstance(exc, RuntimeConnectionError):
                raise
            raise ExecutionOutcomeUnknown(command_id, "command state could not be queried") from exc

    async def start(self, command: Sequence[str], *, command_id: str | None = None,
                    cwd: str = "", env: Mapping[str, str] | None = None,
                    timeout_seconds: int = 0, pty: bool = False,
                    pty_cols: int = 80, pty_rows: int = 24, check: bool = False) -> dict:
        identifier = command_id or uuid.uuid4().hex
        # Ensure/resume the session before every new submission. Reads of an
        # already accepted command reuse the current endpoint until it fails.
        self._endpoint = None
        body = {
            "command_id": identifier, "command": list(command), "cwd": cwd,
            "env": dict(env or {}), "timeout_seconds": timeout_seconds,
            "pty": pty, "pty_cols": pty_cols, "pty_rows": pty_rows, "check": check,
        }
        if self._before_submit is not None:
            await self._before_submit(identifier)
        accepted = await self._request(identifier, "POST", "/v1/commands", body, ambiguous=True)
        if self._command_observer is not None:
            await self._command_observer(identifier, accepted)
        return accepted

    async def get(self, command_id: str) -> dict:
        record = await self._request(command_id, "GET", f"/v1/commands/{command_id}")
        if self._command_observer is not None:
            await self._command_observer(command_id, record)
        return record

    async def read_output(self, command_id: str, *, stream: str = "stdout", cursor: int = 0,
                          limit: int = 64 * 1024) -> OutputPage:
        if stream not in {"stdout", "stderr"} or cursor < 0 or not 1 <= limit <= 1024 * 1024:
            raise ValueError("invalid output stream, cursor, or limit")
        record = await self._request(
            command_id, "GET",
            f"/v1/commands/{command_id}/output?stream={stream}&cursor={cursor}&limit={limit}",
        )
        return OutputPage(command_id, stream, base64.b64decode(record.get("data", ""), validate=True),
                          record["cursor"], record["total_bytes"], record["complete"], record["output_complete"])

    async def wait(self, command_id: str, *, check: bool = False,
                   poll_interval: float | None = None) -> CommandResult:
        while True:
            record = await self.get(command_id)
            if record.get("state") == "unknown":
                raise ExecutionOutcomeUnknown(command_id, record.get("error") or "tracker reports unknown state")
            if record.get("state") == "finished":
                if record.get("output_expired"):
                    raise OutputExpiredError(command_id)
                if record.get("output_error_code") == "session_output_limit":
                    raise OutputStorageLimitExceeded(command_id, record.get("output_error_limit_bytes") or 0)
                if record.get("output_error_code") == "command_output_limit":
                    raise CommandOutputLimitExceeded(command_id, record.get("output_error_limit_bytes") or 0)
                if record.get("stdout_truncated") or record.get("stderr_truncated"):
                    raise CommandOutputLimitExceeded(command_id, self._buffered_response_limit_bytes)
                if not record.get("output_complete", True):
                    raise OutputIncompleteError(command_id, record.get("output_error") or "command output is incomplete")
                code = record.get("exit_code")
                if check and code:
                    raise CommandFailedError(command_id, code)
                return CommandResult(
                    command_id=command_id,
                    exit_code=code,
                    stdout=base64.b64decode(record.get("stdout", ""), validate=True),
                    stderr=base64.b64decode(record.get("stderr", ""), validate=True),
                    stop_reason=record.get("stop_reason"),
                    output_complete=record.get("output_complete", True),
                )
            await asyncio.sleep(poll_interval if poll_interval is not None else self._poll_interval)

    async def run(self, command: Sequence[str], **options) -> CommandResult:
        check = options.get("check", False)
        accepted = await self.start(command, **options)
        return await self.wait(accepted["command_id"], check=check)

    async def cancel(self, command_id: str) -> None:
        await self._request(command_id, "POST", f"/v1/commands/{command_id}/cancel", {})

    async def stdin(self, command_id: str, data: bytes | None) -> None:
        body = {"eof": True} if data is None else {"input": base64.b64encode(data).decode("ascii")}
        await self._request(command_id, "POST", f"/v1/commands/{command_id}/stdin", body)

    async def signal(self, command_id: str, name: str) -> None:
        await self._request(command_id, "POST", f"/v1/commands/{command_id}/signal", {"signal": name})

    async def resize_tty(self, command_id: str, cols: int, rows: int) -> None:
        await self._request(command_id, "POST", f"/v1/commands/{command_id}/resize", {"cols": cols, "rows": rows})


async def _decode_json(payload: bytes, command_id: str) -> dict:
    import json

    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExecutionOutcomeUnknown(command_id, "tracker returned an unreadable response") from exc
    if not isinstance(value, dict):
        raise ExecutionOutcomeUnknown(command_id, "tracker returned an invalid response")
    return value
