import asyncio
from types import SimpleNamespace

import pytest

from silk_agent.sandbox.service_monitor import ServiceCommandRuntime, run_monitor_loop


class Commands:
    def __init__(self, records):
        self.records = records
        self.closed = 0

    async def get(self, command_id):
        record = self.records[command_id]
        if isinstance(record, Exception):
            raise record
        return record

    async def read_output(self, command_id, *, stream, cursor, limit):
        return SimpleNamespace(data=b"out", cursor=3, complete=True, output_complete=True)

    async def close(self):
        self.closed += 1


class Manager:
    def __init__(self, commands):
        self.commands = commands
        self.requested = []

    async def get(self, key):
        self.requested.append(key)
        return SimpleNamespace(commands=self.commands) if self.commands else None


@pytest.mark.asyncio
async def test_status_maps_tracker_states_and_never_starts_a_sandbox():
    commands = Commands(
        {
            "run": {"state": "running"},
            "ok": {"state": "finished", "exit_code": 0},
            "cut": {"state": "finished", "stop_reason": "cancelled"},
            "odd": {"state": "weird"},
            "boom": ConnectionError("tracker"),
        }
    )
    runtime = ServiceCommandRuntime(Manager(commands))
    key = "slack:T:C:1"
    assert (await runtime.command_status(key, "run")).outcome == "running"
    done = await runtime.command_status(key, "ok")
    assert (done.outcome, done.exit_code) == ("completed", 0)
    assert (await runtime.command_status(key, "cut")).outcome == "cancelled"
    assert (await runtime.command_status(key, "odd")).outcome == "unknown"
    assert (await runtime.command_status(key, "boom")).outcome == "unknown"
    assert commands.closed == 5  # every per-call HTTP client is closed
    assert (await ServiceCommandRuntime(Manager(None)).command_status(key, "x")).outcome == (
        "unknown"
    )


@pytest.mark.asyncio
async def test_output_page_requires_an_existing_sandbox_and_reports_completeness():
    runtime = ServiceCommandRuntime(Manager(Commands({})))
    page = await runtime.read_output_page(
        "slack:T:C:1", "c", stream="stdout", cursor=0, limit_bytes=9
    )
    assert (page.data, page.next_cursor, page.complete) == (b"out", 3, True)
    truncated = Commands({})

    async def lossy(_command_id, *, stream, cursor, limit):
        return SimpleNamespace(data=b"out", cursor=3, complete=True, output_complete=False)

    truncated.read_output = lossy
    page = await ServiceCommandRuntime(Manager(truncated)).read_output_page(
        "slack:T:C:1", "c", stream="stdout", cursor=0, limit_bytes=9
    )
    assert page.complete and page.truncated
    with pytest.raises(RuntimeError):
        await ServiceCommandRuntime(Manager(None)).read_output_page(
            "slack:T:C:1", "c", stream="stdout", cursor=0, limit_bytes=9
        )


@pytest.mark.asyncio
async def test_monitor_loop_survives_a_failing_pass_and_stops_on_request():
    calls = []
    stop = asyncio.Event()

    class Monitor:
        async def poll_once(self):
            calls.append(1)
            if len(calls) == 1:
                raise ConnectionError("firestore")
            stop.set()
            return 0

    await asyncio.wait_for(run_monitor_loop(Monitor(), stop, poll_seconds=0.01), 2)
    assert len(calls) == 2
