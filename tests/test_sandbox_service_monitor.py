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


class ExplodingCommands:
    def __getattr__(self, name):
        raise AssertionError("the monitor must not use the ensure()-backed `commands` client")


class Manager:
    def __init__(self, commands, generation=1):
        self.commands = commands
        self.generation = generation
        self.requested = []

    async def get(self, key):
        self.requested.append(key)
        if not self.commands:
            return None
        # `commands` would ensure() the Sandbox; the monitor must only use `observe_commands`.
        return SimpleNamespace(
            commands=ExplodingCommands(), observe_commands=self.commands, generation=self.generation
        )


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
    # No session record at all: its commands are gone.
    assert (await ServiceCommandRuntime(Manager(None)).command_status(key, "x")).outcome == "lost"


@pytest.mark.asyncio
async def test_a_suspended_sandbox_is_not_resumed_and_the_command_stays_unknown():
    from gke_session_sandbox.errors import SandboxNotReadyError

    commands = Commands({"c": SandboxNotReadyError("Sandbox is suspended")})
    result = await ServiceCommandRuntime(Manager(commands)).command_status("slack:T:C:1", "c")
    assert result.outcome == "unknown"  # polled again later, never started


@pytest.mark.asyncio
async def test_the_unavailable_status_log_carries_no_response_body(caplog):
    import json
    import logging

    from gke_session_sandbox.errors import RuntimeConnectionError, SandboxNotReadyError
    from silk_agent.distributed.logging import RoleLogFormatter

    secret = "PRIVATE_RESPONSE_BODY_12345"
    commands = Commands(
        {
            "boom": RuntimeConnectionError(f"tracker returned 500: {secret}"),
            "asleep": SandboxNotReadyError(f"Sandbox is suspended {secret}"),
        }
    )
    runtime = ServiceCommandRuntime(Manager(commands))
    with caplog.at_level(logging.WARNING, logger="silk_agent.sandbox.service_monitor"):
        for command_id in ("boom", "asleep"):
            result = await runtime.command_status("slack:T:C:1", command_id)
            assert result.outcome == "unknown"
    lines = [json.loads(RoleLogFormatter().format(record)) for record in caplog.records]
    assert len(lines) == 2
    assert all(secret not in line["message"] for line in lines)
    assert "sandbox_not_ready" in lines[1]["message"]
    assert "RuntimeConnectionError" in lines[0]["message"]  # the class is enough to tell


@pytest.mark.asyncio
async def test_a_lost_or_replaced_sandbox_ends_the_command():
    from gke_session_sandbox.errors import SandboxLostError, StaleGenerationError

    key = "slack:T:C:1"
    for failure in (SandboxLostError("lost"), StaleGenerationError("older generation")):
        runtime = ServiceCommandRuntime(Manager(Commands({"c": failure})))
        assert (await runtime.command_status(key, "c")).outcome == "lost"
    # The command ran in generation 1; the Sandbox is now generation 2.
    runtime = ServiceCommandRuntime(Manager(Commands({"c": {"state": "running"}}), generation=2))
    assert (await runtime.command_status(key, "c", sandbox_generation=1)).outcome == "lost"
    assert (await runtime.command_status(key, "c", sandbox_generation=2)).outcome == "running"


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


@pytest.mark.asyncio
async def test_a_command_that_hit_an_output_limit_is_counted_and_logged(caplog):
    from silk_agent.distributed.metrics import METRICS

    METRICS.reset()
    commands = Commands(
        {
            "big": {
                "state": "finished",
                "exit_code": 0,
                "output_error_code": "session_output_limit",
            },
            "wide": {
                "state": "finished",
                "exit_code": 0,
                "output_error_code": "command_output_limit",
            },
            "fine": {"state": "finished", "exit_code": 0},
        }
    )
    runtime = ServiceCommandRuntime(Manager(commands))
    with caplog.at_level("WARNING"):
        for command_id in ("big", "wide", "fine"):
            assert (await runtime.command_status("slack:T:C:1", command_id)).outcome == "completed"
    assert METRICS.value("sandbox_output_limit_total", limit="session") == 1
    assert METRICS.value("sandbox_output_limit_total", limit="command") == 1
    assert "hit the session output limit" in caplog.text and "fine" not in caplog.text
    METRICS.reset()
