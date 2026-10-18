import asyncio
import base64

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from silk_agent.sandbox.remote import RemoteSandboxRuntime, RemoteSandboxSessions


async def test_remote_session_client_uses_projected_token_and_read_only_status(tmp_path):
    seen = []

    async def status(request):
        seen.append((request.path, request.headers.get("Authorization")))
        return web.json_response({"exists": True, "ready": True, "state": "ready"})

    app = web.Application()
    app.router.add_get("/v1/sessions/status", status)
    token_file = tmp_path / "token"
    token_file.write_text("projected-token", encoding="utf-8")
    async with TestServer(app) as server:
        remote = RemoteSandboxSessions(str(server.make_url("/")).rstrip("/"), token_file=token_file)
        assert await remote.is_ready("slack:T:C:123")
    assert seen == [("/v1/sessions/status", "Bearer projected-token")]


async def test_remote_file_methods_send_conversation_and_owner_identity(tmp_path):
    seen = []

    async def read(request):
        body = await request.json()
        seen.append((request.path, body))
        return web.json_response({"data_base64": "aGVsbG8="})

    async def write(request):
        body = await request.json()
        seen.append((request.path, body))
        return web.json_response({"state": "written"}, status=202)

    app = web.Application()
    app.router.add_post("/v1/files/read", read)
    app.router.add_put("/v1/files/write", write)
    token_file = tmp_path / "token"
    token_file.write_text("projected-token", encoding="utf-8")
    async with TestServer(app) as server:
        remote = RemoteSandboxSessions(str(server.make_url("/")).rstrip("/"), token_file=token_file)
        identity = dict(owner_epoch=4, conversation_generation=2, turn_id="turn-a", origin_seq=7)
        assert await remote.read_file("slack:T:C:123", "notes.txt", **identity) == b"hello"
        assert (
            await remote.write_file("slack:T:C:123", "notes.txt", b"hello", **identity) == "written"
        )
    assert [path for path, _body in seen] == ["/v1/files/read", "/v1/files/write"]
    assert all(body["conversation_key"] == "slack:T:C:123" for _path, body in seen)
    assert all(body["owner_epoch"] == 4 for _path, body in seen)


@pytest.mark.parametrize("truncated", [False, True])
async def test_remote_runtime_reads_paged_output_for_archive(tmp_path, truncated):
    seen = []

    async def output(request):
        body = await request.json()
        seen.append(body)
        return web.json_response(
            {
                "data_base64": base64.b64encode(b"large-output").decode(),
                "next_cursor": len(b"large-output"),
                "complete": True,
                "truncated": truncated,
            }
        )

    app = web.Application()
    app.router.add_post("/v1/commands/cmd-1/output", output)
    token_file = tmp_path / "token"
    token_file.write_text("token", encoding="utf-8")
    async with TestServer(app) as server:
        sessions = RemoteSandboxSessions(
            str(server.make_url("/")).rstrip("/"), token_file=token_file
        )

        async def identity(_key):
            return {
                "owner_epoch": 2,
                "conversation_generation": 3,
                "turn_id": "turn-1",
                "origin_seq": 4,
            }

        runtime = RemoteSandboxRuntime(sessions, identity)
        page = await runtime.read_output_page(
            "conversation-a", "cmd-1", stream="stderr", cursor=0, limit_bytes=128
        )
    assert page.data == b"large-output" and page.complete and page.truncated is truncated
    assert seen[0]["stream"] == "stderr" and seen[0]["cursor"] == 0


async def test_remote_runtime_wait_cancellation_does_not_cancel_the_command():
    entered_status = asyncio.Event()

    class Sessions:
        canceled = []

        async def start_command(self, conversation_key, command, **kwargs):
            return {"command_id": kwargs["request_id"], "state": "running"}

        async def command_status(self, conversation_key, command_id, **_identity):
            entered_status.set()
            return {"state": "cancelled" if command_id in self.canceled else "running"}

        async def cancel_command(self, conversation_key, command_id, **_kwargs):
            self.canceled.append(command_id)
            return {"state": "cancel_requested"}

    async def identity(_conversation_key):
        return {
            "owner_epoch": 2,
            "conversation_generation": 3,
            "turn_id": "turn-1",
            "origin_seq": 4,
        }

    sessions = Sessions()
    runtime = RemoteSandboxRuntime(sessions, identity, poll_interval=0.01)
    started = []

    async def on_started(command_id):
        started.append(command_id)

    task = asyncio.create_task(
        runtime.run_command("conversation-a", "sleep 60", on_started=on_started)
    )
    await asyncio.wait_for(entered_status.wait(), timeout=1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("wait cancellation must propagate")
    assert len(started) == 1
    assert sessions.canceled == []
    assert (await runtime.command_status("conversation-a", started[0])).outcome == "running"
    await runtime.cancel_commands("conversation-a")
    assert sessions.canceled == [started[0]]
