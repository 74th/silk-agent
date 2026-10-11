from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timedelta, timezone
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gke_session_sandbox.tracker import CommandJournal, Conflict, GateClosed, Tracker, HTTPGateClosed
from gke_session_sandbox import tracker as tracker_module
from gke_session_sandbox import supervisor
from gke_session_sandbox.errors import CommandOutputLimitExceeded, OutputExpiredError, OutputStorageLimitExceeded


@pytest.mark.asyncio
async def test_journal_persists_idempotency_and_gate(tmp_path):
    path = tmp_path / "commands.json"
    journal = CommandJournal(path)
    request = {"command_id": "repeat", "command": ["echo", "ok"]}
    _, created = await journal.reserve(request)
    assert created
    _, created = await journal.reserve(request)
    assert not created
    with pytest.raises(Conflict):
        await journal.reserve({**request, "command": ["echo", "different"]})
    await journal.set_gate(False)
    with pytest.raises(GateClosed):
        await journal.reserve({"command_id": "new", "command": ["true"]})

    reopened = CommandJournal(path)
    assert reopened.accepting is False
    assert (await reopened.get("repeat"))["state"] == "reserved"


@pytest.mark.asyncio
async def test_gate_closed_uses_http_423(tmp_path):
    journal = CommandJournal(tmp_path / "commands.json")
    await journal.set_gate(False)
    tracker = Tracker(journal, object(), generation=1)
    app = web.Application()
    app.router.add_post("/commands", tracker.create)
    async with TestClient(TestServer(app)) as client:
        response = await client.post("/commands", json={"command_id": "blocked", "command": ["true"]})
        assert response.status == 423
        assert "gate is closed" in await response.text()


@pytest.mark.asyncio
async def test_tracker_stream_records_output_and_check_failure(tmp_path):
    journal = CommandJournal(tmp_path / "commands.json")
    request = {"command_id": "exit-seven", "command": ["sh", "-c", "exit 7"], "check": True}
    record, _ = await journal.reserve(request)

    class Event:
        def __init__(self, kind, value):
            self.kind, self.value = kind, value

        def WhichOneof(self, _):
            return self.kind

        @property
        def init(self):
            return self.value

        @property
        def stdout(self):
            return self.value

        @property
        def stderr(self):
            return self.value

        @property
        def exit(self):
            return self.value

    class Stream:
        async def __aiter__(self):
            yield Event("init", type("Init", (), {"process_id": 51})())
            yield Event("stdout", b"out\n")
            yield Event("stderr", b"err\n")
            yield Event("exit", type("Exit", (), {"exit_code": 7})())

    class Stub:
        def Start(self, _):
            return Stream()

        async def Execute(self, request, timeout):
            class Response:
                exit_code = 1
            return Response()

    tracker = Tracker(journal, Stub(), generation=1)
    tracker._read_identity = _identity_missing
    await journal.update(record["command_id"], state="running", process_id=51)
    await tracker._consume(record["command_id"], Stream(), "marker")
    saved = await journal.get(record["command_id"])
    assert saved["state"] == "finished"
    assert saved["exit_code"] == 7
    assert base64.b64decode((await journal.read_output(record["command_id"], "stdout", 0, 100))["data"]) == b"out\n"
    assert base64.b64decode((await journal.read_output(record["command_id"], "stderr", 0, 100))["data"]) == b"err\n"
    assert saved["error"] == "command exited with status 7"


@pytest.mark.asyncio
async def test_journal_persists_output_chunks_and_replays_by_cursor(tmp_path):
    journal = CommandJournal(tmp_path / "commands.json", buffered_response_limit_bytes=4)
    await journal.reserve({"command_id": "paged", "command": ["printf", "large"]})
    await journal.append_output("paged", "stdout", b"abcdefghij")
    await journal.update("paged", state="finished", finished_at="2026-10-10T00:00:00+00:00")

    record = await journal.get("paged")
    first = await journal.read_output("paged", "stdout", 0, 3)
    second = await journal.read_output("paged", "stdout", first["cursor"], 3)
    third = await journal.read_output("paged", "stdout", second["cursor"], 20)
    assert record["stdout_truncated"] is True
    assert record["stdout_bytes"] == 10
    assert record["stdout"] == ""
    assert b"".join(base64.b64decode(page["data"]) for page in (first, second, third)) == b"abcdefghij"
    assert third["complete"] is True


@pytest.mark.asyncio
async def test_journal_enforces_command_and_session_output_storage_limits(tmp_path):
    journal = CommandJournal(tmp_path / "commands.json", command_output_limit_bytes=5,
                             session_output_limit_bytes=8, buffered_response_limit_bytes=8)
    await journal.reserve({"command_id": "one", "command": ["printf", "one"]})
    await journal.reserve({"command_id": "two", "command": ["printf", "two"]})
    await journal.append_output("one", "stdout", b"12345")
    with pytest.raises(CommandOutputLimitExceeded):
        await journal.append_output("one", "stderr", b"x")
    await journal.append_output("two", "stdout", b"123")
    with pytest.raises(OutputStorageLimitExceeded):
        await journal.append_output("two", "stderr", b"x")
    assert (await journal.get("one"))["output_complete"] is False
    assert (await journal.get("two"))["output_error_code"] == "session_output_limit"


@pytest.mark.asyncio
async def test_tracker_stops_on_output_limit_but_keeps_consuming_until_exit(tmp_path):
    journal = CommandJournal(tmp_path / "commands.json", command_output_limit_bytes=2,
                             session_output_limit_bytes=10, buffered_response_limit_bytes=2)
    await journal.reserve({"command_id": "limited", "command": ["printf", "large"]})

    class Event:
        def __init__(self, kind, value):
            self.kind, self.value = kind, value
        def WhichOneof(self, _): return self.kind
        @property
        def init(self): return self.value
        @property
        def stdout(self): return self.value
        @property
        def exit(self): return self.value

    consumed = []
    class Stream:
        async def __aiter__(self):
            for event in (
                Event("init", type("Init", (), {"process_id": 51})()),
                Event("stdout", b"abc"),
                Event("stdout", b"ignored-after-limit"),
                Event("exit", type("Exit", (), {"exit_code": 0})()),
            ):
                consumed.append(event.kind)
                yield event

    class Stub:
        def Start(self, _): return Stream()
    tracker = Tracker(journal, Stub(), generation=1)
    tracker._read_identity = _identity_missing
    stops = []
    async def stop(command_id, reason): stops.append((command_id, reason))
    tracker.stop = stop
    await journal.update("limited", state="running", process_id=51)
    await tracker._consume("limited", Stream(), "marker")
    record = await journal.get("limited")
    assert consumed == ["init", "stdout", "stdout", "exit"]
    assert stops == [("limited", "output_storage_error")]
    assert record["state"] == "finished" and record["output_complete"] is False
    assert record["output_error_code"] == "command_output_limit"
    page = await journal.read_output("limited", "stdout", 0, 10)
    assert base64.b64decode(page["data"]) == b"ab"
    assert record["output_missing_bytes"] == len(b"abc") - 2


@pytest.mark.asyncio
async def test_tracker_reports_output_file_write_failure_separately_from_quota(tmp_path, monkeypatch):
    journal = CommandJournal(tmp_path / "commands.json")
    await journal.reserve({"command_id": "disk-error", "command": ["printf", "large"]})
    output_path = tmp_path / "command-output" / "disk-error.stdout"
    original_open = type(output_path).open

    def fail_output_open(path, mode="r", *args, **kwargs):
        if path == output_path and mode == "ab":
            raise OSError("simulated PVC write failure")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(type(output_path), "open", fail_output_open)

    class Event:
        def __init__(self, kind, value):
            self.kind, self.value = kind, value
        def WhichOneof(self, _): return self.kind
        @property
        def init(self): return self.value
        @property
        def stdout(self): return self.value
        @property
        def exit(self): return self.value

    class Stream:
        async def __aiter__(self):
            yield Event("init", type("Init", (), {"process_id": 52})())
            yield Event("stdout", b"not-saved")
            yield Event("exit", type("Exit", (), {"exit_code": 0})())

    class Stub:
        def Start(self, _): return Stream()

    tracker = Tracker(journal, Stub(), generation=1)
    tracker._read_identity = _identity_missing
    stops = []
    async def stop(command_id, reason): stops.append((command_id, reason))
    tracker.stop = stop
    await journal.update("disk-error", state="running", process_id=52)
    await tracker._consume("disk-error", Stream(), "marker")

    record = await journal.get("disk-error")
    assert stops == [("disk-error", "output_storage_error")]
    assert record["state"] == "finished"
    assert record["output_error_code"] == "output_storage_error"
    assert record["output_complete"] is False
    assert record["stdout_bytes"] == 0


@pytest.mark.asyncio
async def test_journal_expires_files_but_keeps_finished_summary(tmp_path):
    now = [datetime(2026, 10, 10, tzinfo=timezone.utc)]
    journal = CommandJournal(tmp_path / "commands.json", output_retention_seconds=10, clock=lambda: now[0])
    await journal.reserve({"command_id": "expired", "command": ["printf", "x"]})
    await journal.append_output("expired", "stdout", b"saved")
    await journal.update("expired", state="finished", finished_at=now[0].isoformat(), exit_code=0)

    now[0] += timedelta(seconds=9)
    assert await journal.expire_outputs() == 0
    now[0] += timedelta(seconds=1)
    assert await journal.expire_outputs() == 1

    record = await journal.get("expired")
    assert record["state"] == "finished" and record["exit_code"] == 0
    assert record["output_expired"] is True
    assert not (tmp_path / "command-output" / "expired.stdout").exists()
    with pytest.raises(OutputExpiredError):
        await journal.read_output("expired", "stdout", 0, 10)


@pytest.mark.asyncio
async def test_tracker_resolves_virtual_process_id_to_os_pid(tmp_path):
    calls = 0

    class Stub:
        async def Execute(self, request, timeout):
            nonlocal calls
            calls += 1
            command = request.config.command
            class Response:
                exit_code = 0
                stdout = (b"4242 4242 (sleep with spaces) S 1 4242 4242 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 9876\n"
                          if calls == 1 else b"boot-id\n")
            if calls == 1:
                assert command[0:2] == ["/bin/sh", "-c"]
                assert "GKE_SESSION_SANDBOX_TRACKER_MARKER=abc123" in command[2]
            return Response()

    tracker = Tracker(CommandJournal(tmp_path / "commands.json"), Stub(), 1)
    identity = await tracker._read_identity(17, "abc123")
    assert identity == {"process_id": 17, "pid": 4242, "pgid": 4242, "start_time": 9876, "boot_id": "boot-id"}


async def _identity_missing(_pid, _marker):
    return None


@pytest.mark.asyncio
async def test_tracker_stop_escalates_to_kill(monkeypatch, tmp_path):
    journal = CommandJournal(tmp_path / "commands.json")
    await journal.reserve({"command_id": "cancel-me", "command": ["sleep", "60"]})
    await journal.update("cancel-me", state="running", process_id=42)
    sent = []
    tracker = Tracker(journal, object(), generation=1)
    async def record_signal(_pid, value):
        sent.append(value)
    tracker._send_signal = record_signal
    original_sleep = asyncio.sleep
    async def immediate_sleep(_seconds):
        await original_sleep(0)
    monkeypatch.setattr(tracker_module.asyncio, "sleep", immediate_sleep)
    await tracker.stop("cancel-me", "cancelled")
    await original_sleep(0.01)
    assert sent == [tracker_module.process_pb2.SIGNAL_SIGTERM, tracker_module.process_pb2.SIGNAL_SIGKILL]
    assert (await journal.get("cancel-me"))["stop_reason"] == "cancelled"


@pytest.mark.asyncio
async def test_process_group_probe_excludes_its_reused_own_pgid(tmp_path):
    class Stub:
        async def Execute(self, request, timeout):
            command = request.config.command
            assert command[0:2] == ["/bin/sh", "-c"]
            assert command[2].startswith('[ "$$" -ne 42 ] || exit 1; ps -eo pid=,pgid=,comm= | awk')
            class Response:
                exit_code = 1
            return Response()

    assert await Tracker(CommandJournal(tmp_path / "commands.json"), Stub(), 1)._group_present(42) == (False, "")


@pytest.mark.asyncio
async def test_supervisor_stops_tracker_before_sandboxd(monkeypatch):
    events = []

    class Child:
        def __init__(self, name):
            self.name, self.returncode = name, None
        async def wait(self):
            await asyncio.Future()
        def terminate(self):
            events.append(f"terminate:{self.name}")
            self.returncode = -15
        def kill(self):
            events.append(f"kill:{self.name}")
            self.returncode = -9

    children = iter((Child("sandboxd"), Child("tracker")))
    async def spawn(*_args):
        return next(children)
    async def ready(_process, timeout=45):
        events.append("ready")
    async def stop(process, grace=5):
        events.append(f"stop:{process.name}")
        process.returncode = -15
    monkeypatch.setattr(supervisor.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(supervisor, "_wait_ready", ready)
    monkeypatch.setattr(supervisor, "_stop", stop)
    event = asyncio.Event()
    task = asyncio.create_task(supervisor.run(stop_event=event, sandboxd_command=["sandboxd"], tracker_command=["tracker"]))
    await asyncio.sleep(0)
    event.set()
    assert await task == 0
    assert events == ["ready", "stop:tracker", "stop:sandboxd"]
