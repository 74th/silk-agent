from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gke_session_sandbox.files import FileClient
from gke_session_sandbox.errors import FileTransferLimitExceeded, WorkspaceReadOnlyError


@pytest.mark.asyncio
async def test_streaming_file_api_round_trip_and_metadata():
    files: dict[str, bytes] = {"folder/readme.txt": b"hello"}

    async def handle(request):
        path = request.match_info["path"]
        if request.method == "PUT":
            files[path] = await request.read()
            return web.Response(status=204)
        if request.method == "DELETE":
            files.pop(path, None)
            return web.Response(status=204)
        if path == "folder" and request.method == "GET":
            return web.json_response({"path": "/folder", "entries": [{
                "name": "readme.txt", "type": "file", "size": 5,
                "modified_at": "2026-10-10T00:00:00Z", "mode": "0644",
            }]})
        if request.method == "HEAD":
            if path not in files:
                return web.Response(status=404)
            return web.Response(headers={"Content-Length": str(len(files[path])), "Content-Type": "application/octet-stream"})
        if path not in files:
            return web.Response(status=404)
        return web.Response(body=files[path], content_type="application/octet-stream")

    app = web.Application()
    app.router.add_route("*", "/v1/files/{path:.*}", handle)
    async with TestClient(TestServer(app)) as server:
        endpoint = str(server.make_url("")).rstrip("/")
        chunks = []
        async with FileClient(lambda: _resolved(endpoint)) as client:
            assert await client.read("folder/readme.txt") == b"hello"
            assert await client.read_to("folder/readme.txt", chunks.append) == 5
            assert b"".join(chunks) == b"hello"
            assert await client.exists("folder/readme.txt")
            assert (await client.stat("folder/readme.txt")).size == 5
            entries = await client.list("folder")
            assert entries[0].path == "folder/readme.txt"
            async def source():
                yield b"stream-"
                yield b"write"
            assert await client.write_from("folder/new.txt", source()) == 12
            assert files["folder/new.txt"] == b"stream-write"
            await client.delete("folder/new.txt")
            assert not await client.exists("folder/new.txt")


@pytest.mark.asyncio
async def test_file_client_rejects_unsafe_paths_limits_and_read_only_writes():
    async def missing(_request):
        return web.Response(status=404)
    app = web.Application()
    app.router.add_route("*", "/v1/files/{path:.*}", missing)
    async with TestClient(TestServer(app)) as server:
        endpoint = str(server.make_url("")).rstrip("/")
        async with FileClient(lambda: _resolved(endpoint), response_limit_bytes=2) as client:
            with pytest.raises(ValueError, match="traversal"):
                await client.read("../outside")
            with pytest.raises(ValueError, match="reserved"):
                await client.delete(".session-runtime/commands.json")
        async with FileClient(lambda: _resolved(endpoint), read_only=True) as client:
            with pytest.raises(WorkspaceReadOnlyError):
                await client.write("blocked", b"data")

    async def big(_request):
        response = web.StreamResponse(headers={"Content-Type": "application/octet-stream"})
        await response.prepare(_request)
        await response.write(b"abc")
        await response.write_eof()
        return response
    app = web.Application()
    app.router.add_get("/v1/files/big", big)
    async with TestClient(TestServer(app)) as server:
        endpoint = str(server.make_url("")).rstrip("/")
        async with FileClient(lambda: _resolved(endpoint), response_limit_bytes=2) as client:
            with pytest.raises(FileTransferLimitExceeded):
                await client.read("big")


@pytest.mark.asyncio
async def test_file_activity_hook_covers_streaming_and_nested_metadata_operations():
    async def handle(_request):
        return web.json_response({"entries": [{
            "name": "file.txt", "type": "file", "size": 1,
            "modified_at": "2026-10-10T00:00:00Z", "mode": "0644",
        }]})

    app = web.Application()
    app.router.add_get("/v1/files/folder", handle)
    async with TestServer(app) as server:
        activity: list[int] = []
        endpoint = str(server.make_url("" )).rstrip("/")
        async with FileClient(
            lambda: _resolved(endpoint), activity_hook=lambda delta: _record_delta(activity, delta)
        ) as client:
            assert (await client.stat("folder/file.txt")).size == 1
        assert activity == [1, 1, -1, -1]


async def _resolved(endpoint: str) -> str:
    return endpoint


async def _record_delta(values: list[int], delta: int) -> None:
    values.append(delta)
