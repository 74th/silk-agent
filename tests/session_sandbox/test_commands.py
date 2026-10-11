from __future__ import annotations

import asyncio
import base64

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gke_session_sandbox.commands import CommandTrackerClient
from gke_session_sandbox.errors import (
    CommandFailedError, CommandOutputLimitExceeded, ExecutionOutcomeUnknown, RuntimeConnectionError,
)


@pytest.mark.asyncio
async def test_run_reserves_once_and_returns_saved_output():
    calls = {"post": 0, "get": 0, "resolve": 0}
    state = None

    async def create(request):
        nonlocal state
        calls["post"] += 1
        state = await request.json()
        return web.json_response({**state, "state": "running"}, status=202)

    async def get(_request):
        calls["get"] += 1
        if calls["get"] == 1:
            return web.json_response({**state, "state": "running"})
        return web.json_response({**state, "state": "finished", "exit_code": 0,
                                  "stdout": base64.b64encode(b"done\n").decode(), "stderr": ""})

    app = web.Application()
    app.router.add_post("/v1/commands", create)
    app.router.add_get("/v1/commands/{command_id}", get)
    async with TestClient(TestServer(app)) as server:
        async def resolve():
            calls["resolve"] += 1
            return str(server.make_url("")).rstrip("/")
        async with CommandTrackerClient(resolve, poll_interval=0.001) as commands:
            result = await commands.run(["echo", "done"])
        assert result.stdout == b"done\n"
        assert result.command_id == state["command_id"]
    assert calls == {"post": 1, "get": 2, "resolve": 1}


@pytest.mark.asyncio
async def test_unknown_submission_is_not_retried_and_keeps_command_id():
    calls = 0
    received = asyncio.Event()

    async def create(_request):
        nonlocal calls
        calls += 1
        received.set()
        await asyncio.sleep(1)
        return web.json_response({})

    app = web.Application()
    app.router.add_post("/v1/commands", create)
    async with TestClient(TestServer(app)) as server:
        endpoint = str(server.make_url("")).rstrip("/")
        async with CommandTrackerClient(lambda: _resolved(endpoint), request_timeout=0.02) as commands:
            with pytest.raises(ExecutionOutcomeUnknown) as error:
                await commands.start(["touch", "/workspace/side-effect"], command_id="known-id")
            assert error.value.command_id == "known-id"
        assert received.is_set()
    assert calls == 1


@pytest.mark.asyncio
async def test_known_tracker_rejection_is_not_reported_as_unknown():
    async def conflict(_request):
        return web.json_response({"error": "command ID conflicts"}, status=409)

    app = web.Application()
    app.router.add_post("/v1/commands", conflict)
    async with TestClient(TestServer(app)) as server:
        endpoint = str(server.make_url("")).rstrip("/")
        async with CommandTrackerClient(lambda: _resolved(endpoint)) as commands:
            with pytest.raises(RuntimeConnectionError, match="HTTP 409"):
                await commands.start(["true"], command_id="conflict-id")


@pytest.mark.asyncio
async def test_read_output_uses_byte_cursor_and_buffered_wait_reports_limit():
    async def output(request):
        assert request.query["stream"] == "stdout"
        assert request.query["cursor"] == "2"
        return web.json_response({"command_id": "paged", "stream": "stdout", "data": base64.b64encode(b"cd").decode(),
                                  "cursor": 4, "total_bytes": 6, "complete": False, "output_complete": True})

    async def get(_request):
        return web.json_response({"command_id": "large", "state": "finished", "exit_code": 0,
                                  "stdout": "", "stderr": "", "stdout_truncated": True})

    app = web.Application()
    app.router.add_get("/v1/commands/paged/output", output)
    app.router.add_get("/v1/commands/large", get)
    async with TestClient(TestServer(app)) as server:
        endpoint = str(server.make_url("")).rstrip("/")
        async with CommandTrackerClient(lambda: _resolved(endpoint), buffered_response_limit_bytes=4) as commands:
            page = await commands.read_output("paged", cursor=2, limit=2)
            assert page.data == b"cd" and page.cursor == 4 and page.total_bytes == 6
            with pytest.raises(CommandOutputLimitExceeded) as error:
                await commands.wait("large")
            assert error.value.command_id == "large" and error.value.limit_bytes == 4


async def _resolved(value: str) -> str:
    return value


@pytest.mark.asyncio
async def test_reconnects_and_check_error_keeps_command_id():
    calls = 0

    async def get(_request):
        return web.json_response({"command_id": "failed-id", "state": "finished", "exit_code": 7,
                                  "stdout": "", "stderr": ""})

    app = web.Application()
    app.router.add_get("/v1/commands/failed-id", get)
    async with TestClient(TestServer(app)) as server:
        endpoint = str(server.make_url("")).rstrip("/")
        async def resolve():
            nonlocal calls
            calls += 1
            return "http://127.0.0.1:1" if calls == 1 else endpoint
        async with CommandTrackerClient(resolve) as commands:
            with pytest.raises(ExecutionOutcomeUnknown):
                await commands.get("failed-id")
            with pytest.raises(CommandFailedError) as error:
                await commands.wait("failed-id", check=True)
            assert error.value.command_id == "failed-id"
            assert error.value.exit_code == 7
    assert calls == 2
