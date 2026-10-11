"""Async file client for the sandboxd REST filesystem API."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from urllib.parse import quote

import aiohttp

from .errors import FileOperationError, FileTransferLimitExceeded, WorkspaceReadOnlyError

EndpointResolver = Callable[[], Awaitable[str]]
FileActivityHook = Callable[[int], Awaitable[None]]


def _tracks_activity(method):
    async def wrapped(self, *args, **kwargs):
        if self._activity_hook is not None:
            await self._activity_hook(1)
        try:
            return await method(self, *args, **kwargs)
        finally:
            if self._activity_hook is not None:
                await self._activity_hook(-1)
    wrapped.__name__ = method.__name__
    wrapped.__doc__ = method.__doc__
    return wrapped


@dataclass(frozen=True, slots=True)
class FileInfo:
    path: str
    type: str
    size: int | None = None
    modified_at: datetime | None = None
    mode: str | None = None


class FileClient:
    """Read and write workspace files over sandboxd REST without gRPC access."""

    def __init__(self, endpoint_resolver: EndpointResolver, *, read_only: bool = False,
                 response_limit_bytes: int = 8 * 1024 * 1024,
                 transfer_limit_bytes: int = 64 * 1024 * 1024,
                 request_timeout: float = 30, activity_hook: FileActivityHook | None = None) -> None:
        if min(response_limit_bytes, transfer_limit_bytes) <= 0 or request_timeout <= 0:
            raise ValueError("file limits and request_timeout must be positive")
        self._endpoint_resolver = endpoint_resolver
        self._read_only = read_only
        self._response_limit = response_limit_bytes
        self._transfer_limit = transfer_limit_bytes
        self._request_timeout = request_timeout
        self._activity_hook = activity_hook
        self._http_session: aiohttp.ClientSession | None = None
        self._base: str | None = None

    async def __aenter__(self) -> "FileClient":
        self._http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._request_timeout))
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.close()

    async def close(self) -> None:
        if self._http_session is not None:
            await self._http_session.close()
            self._http_session = None

    def _http(self) -> aiohttp.ClientSession:
        if self._http_session is None:
            self._http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._request_timeout))
        return self._http_session

    async def _url(self, path: str) -> str:
        relative = _workspace_path(path)
        if self._base is None:
            self._base = (await self._endpoint_resolver()).rstrip("/")
        return f"{self._base}/v1/files/{quote(relative, safe='/')}"

    @_tracks_activity
    async def read(self, path: str) -> bytes:
        url = await self._url(path)
        async with self._http().get(url) as response:
            await self._check(response, path)
            if response.content_type == "application/json":
                raise FileOperationError("path is a directory; use list()", path=path, status=400)
            chunks = bytearray()
            async for chunk in response.content.iter_chunked(64 * 1024):
                if len(chunks) + len(chunk) > self._response_limit:
                    raise FileTransferLimitExceeded(
                        f"buffered read exceeds {self._response_limit} bytes; use read_to()"
                    )
                chunks.extend(chunk)
            return bytes(chunks)

    @_tracks_activity
    async def read_to(self, path: str, sink: Callable[[bytes], object] | object) -> int:
        """Stream file chunks to a callback or binary writer and return bytes copied."""
        url = await self._url(path)
        copied = 0
        async with self._http().get(url) as response:
            await self._check(response, path)
            if response.content_type == "application/json":
                raise FileOperationError("path is a directory; use list()", path=path, status=400)
            async for chunk in response.content.iter_chunked(64 * 1024):
                copied += len(chunk)
                if copied > self._transfer_limit:
                    raise FileTransferLimitExceeded(f"streamed read exceeds {self._transfer_limit} bytes")
                writer = sink if callable(sink) else getattr(sink, "write")
                result = writer(chunk)
                if inspect.isawaitable(result):
                    await result
        return copied

    @_tracks_activity
    async def write(self, path: str, data: bytes, *, mode: str | None = None) -> None:
        self._ensure_writable(path)
        if len(data) > self._transfer_limit:
            raise FileTransferLimitExceeded(f"write exceeds {self._transfer_limit} bytes; use write_from()")
        await self._put(path, data, mode=mode)

    @_tracks_activity
    async def write_from(self, path: str, source: AsyncIterable[bytes] | Iterable[bytes], *,
                         mode: str | None = None) -> int:
        self._ensure_writable(path)
        copied = 0

        async def chunks():
            nonlocal copied
            iterator = source.__aiter__() if hasattr(source, "__aiter__") else _iterate_sync(source)
            async for chunk in iterator:
                if not isinstance(chunk, bytes):
                    raise TypeError("write_from source must yield bytes")
                copied += len(chunk)
                if copied > self._transfer_limit:
                    raise FileTransferLimitExceeded(f"streamed write exceeds {self._transfer_limit} bytes")
                yield chunk

        await self._put(path, chunks(), mode=mode)
        return copied

    async def _put(self, path: str, data, *, mode: str | None) -> None:
        url = await self._url(path)
        params = {"mode": mode} if mode is not None else None
        async with self._http().put(url, data=data, params=params) as response:
            await self._check(response, path, expected={204})

    @_tracks_activity
    async def list(self, path: str = ".") -> tuple[FileInfo, ...]:
        url = await self._url(path)
        async with self._http().get(url) as response:
            await self._check(response, path, expected={200})
            try:
                listing = await response.json()
                entries = listing["entries"]
                return tuple(FileInfo(
                    path=str(PurePosixPath(_workspace_path(path)) / entry["name"]),
                    type=entry["type"], size=int(entry["size"]),
                    modified_at=datetime.fromisoformat(entry["modified_at"].replace("Z", "+00:00")),
                    mode=entry.get("mode"),
                ) for entry in entries if entry.get("name") != ".session-runtime")
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise FileOperationError("sandboxd returned an invalid directory listing", path=path) from exc

    @_tracks_activity
    async def stat(self, path: str) -> FileInfo:
        relative = _workspace_path(path)
        if relative == ".":
            return FileInfo(path=relative, type="directory")
        parent = str(PurePosixPath(relative).parent)
        name = PurePosixPath(relative).name
        for entry in await self.list(parent):
            if PurePosixPath(entry.path).name == name:
                return entry
        raise FileOperationError("path does not exist", path=path, status=404)

    @_tracks_activity
    async def exists(self, path: str) -> bool:
        url = await self._url(path)
        async with self._http().head(url) as response:
            if response.status == 404:
                return False
            await self._check(response, path, expected={200})
            return True

    @_tracks_activity
    async def delete(self, path: str, *, recursive: bool = False) -> None:
        self._ensure_writable(path)
        relative = _workspace_path(path)
        if relative in {"", "."}:
            raise ValueError("workspace root cannot be deleted")
        url = await self._url(path)
        async with self._http().delete(url, params={"recursive": "true"} if recursive else None) as response:
            await self._check(response, path, expected={204})

    def _ensure_writable(self, path: str) -> None:
        if self._read_only:
            raise WorkspaceReadOnlyError(f"workspace is read-only: {path}")

    @staticmethod
    async def _check(response: aiohttp.ClientResponse, path: str, *, expected: set[int] | None = None) -> None:
        expected = expected or {200}
        if response.status not in expected:
            detail = (await response.read()).decode(errors="replace")
            raise FileOperationError(detail or f"sandboxd returned HTTP {response.status}",
                                     path=path, status=response.status)


async def _iterate_sync(source: Iterable[bytes]):
    for chunk in source:
        yield chunk


def _workspace_path(value: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or value.startswith("/"):
        raise ValueError("workspace path must be a nonempty relative path")
    path = PurePosixPath(value)
    parts = path.parts
    if any(part in {"..", ""} for part in parts):
        raise ValueError("workspace path traversal is not allowed")
    if parts and parts[0] == ".session-runtime":
        raise ValueError(".session-runtime is reserved for durable tracker state")
    normalized = str(path)
    return "." if normalized == "" else normalized
