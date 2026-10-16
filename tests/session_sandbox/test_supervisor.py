"""The supervisor's startup script: run before sandboxd, with the Sandbox's environment."""

from __future__ import annotations

import asyncio
import stat
import time

import pytest

from gke_session_sandbox import supervisor


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A sandbox root, a log shared by the script and the fake children, and a script path."""
    root = tmp_path / "workspace"
    root.mkdir()
    monkeypatch.setenv("SANDBOXD_ROOT_DIR", str(root))
    monkeypatch.setenv("SANDBOX_STARTUP_SCRIPT", str(tmp_path / "startup.sh"))
    monkeypatch.delenv("SANDBOX_STARTUP_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("SANDBOX_STARTUP_ON_ERROR", raising=False)
    monkeypatch.delenv("AGENT_USER_EMAIL", raising=False)

    log = tmp_path / "log"
    log.touch()

    async def ready(_process, limit_seconds=45):
        # Stand-in for the readiness probe: production starts the tracker only once sandboxd
        # answers. Without waiting for the fake sandboxd's first log line, the two shells race
        # to write theirs and the start order the tests assert is not guaranteed.
        deadline = time.monotonic() + 10
        while "sandboxd" not in log.read_text():
            assert time.monotonic() < deadline, "the fake sandboxd did not start"
            await asyncio.sleep(0.01)

    monkeypatch.setattr(supervisor, "_wait_ready", ready)
    return type("Env", (), {"root": root, "script": tmp_path / "startup.sh", "log": log})


def write_script(env, body: str, *, executable: bool = True) -> None:
    env.script.write_text("#!/bin/sh\n" + body, encoding="utf-8")
    if executable:
        env.script.chmod(env.script.stat().st_mode | stat.S_IXUSR)


def children(env):
    """Fake sandboxd / tracker that record that they started, then wait to be stopped."""
    sandboxd = [
        "/bin/sh", "-c", f"echo sandboxd >> {env.log}; exec sleep 60",
    ]  # fmt: skip
    tracker = ["/bin/sh", "-c", f"echo tracker >> {env.log}; exec sleep 60"]
    return sandboxd, tracker


async def run_until_children_start(env, **kw) -> int:
    sandboxd, tracker = children(env)
    stop = asyncio.Event()
    task = asyncio.create_task(
        supervisor.run(sandboxd_command=sandboxd, tracker_command=tracker, stop_event=stop, **kw)
    )
    deadline = time.monotonic() + 10
    while "tracker" not in env.log.read_text() and not task.done():
        assert time.monotonic() < deadline, "the children did not start"
        await asyncio.sleep(0.02)
    stop.set()
    return await asyncio.wait_for(task, 10)


def lines(env) -> list[str]:
    return env.log.read_text().split()


async def test_without_a_script_the_children_just_start(env):
    assert await run_until_children_start(env) == 0
    assert lines(env) == ["sandboxd", "tracker"]
    assert not (env.root / ".session-runtime" / "startup-done").exists()


async def test_the_script_runs_to_the_end_before_sandboxd_starts(env):
    write_script(env, f"sleep 0.3; echo startup >> {env.log}\n")
    assert await run_until_children_start(env) == 0
    assert lines(env) == ["startup", "sandboxd", "tracker"]


async def test_the_script_runs_in_the_sandbox_root_with_the_containers_env(env, monkeypatch):
    monkeypatch.setenv("AGENT_USER_EMAIL", "alice@example.com")
    write_script(env, f'echo "$PWD $AGENT_USER_EMAIL" >> {env.log}\n')
    await run_until_children_start(env)
    pwd, email = env.log.read_text().splitlines()[0].split()
    assert pwd == str(env.root) and email == "alice@example.com"


async def test_a_script_without_the_execute_bit_is_run_by_sh(env):
    write_script(env, f"echo startup >> {env.log}\n", executable=False)
    await run_until_children_start(env)
    assert lines(env)[0] == "startup"


async def test_first_run_is_1_then_0(env):
    write_script(env, f'echo "first=$SANDBOX_STARTUP_FIRST_RUN" >> {env.log}\n')
    marker = env.root / ".session-runtime" / "startup-done"
    seen = []
    for expected_marker in (True, True):  # create, then resume
        env.log.write_text("")
        await run_until_children_start(env)
        seen.append(lines(env)[0])
        assert marker.exists() is expected_marker
    marker.unlink()  # the workspace was recreated
    env.log.write_text("")
    await run_until_children_start(env)
    seen.append(lines(env)[0])
    assert seen == ["first=1", "first=0", "first=1"]


async def test_a_failing_script_stops_the_start_up_by_default(env, capsys):
    write_script(env, f"echo startup >> {env.log}; exit 3\n")
    sandboxd, tracker = children(env)
    with pytest.raises(RuntimeError, match="exit code 3"):
        await supervisor.run(
            sandboxd_command=sandboxd, tracker_command=tracker, stop_event=asyncio.Event()
        )
    assert lines(env) == ["startup"]  # sandboxd never started
    assert "exit code 3" in capsys.readouterr().err
    assert not (env.root / ".session-runtime" / "startup-done").exists()


async def test_a_failing_script_can_be_ignored(env, monkeypatch, capsys):
    monkeypatch.setenv("SANDBOX_STARTUP_ON_ERROR", "continue")
    write_script(env, f"echo startup >> {env.log}; exit 1\n")
    assert await run_until_children_start(env) == 0
    assert lines(env) == ["startup", "sandboxd", "tracker"]
    assert "exit code 1" in capsys.readouterr().err
    # A failed run does not count as the first run being done.
    assert not (env.root / ".session-runtime" / "startup-done").exists()


async def test_a_script_that_takes_too_long_is_killed(env, monkeypatch):
    monkeypatch.setenv("SANDBOX_STARTUP_TIMEOUT_SECONDS", "0.5")
    write_script(env, f"echo startup >> {env.log}; sleep 30; echo late >> {env.log}\n")
    sandboxd, tracker = children(env)
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="did not finish within 0.5 seconds"):
        await supervisor.run(
            sandboxd_command=sandboxd, tracker_command=tracker, stop_event=asyncio.Event()
        )
    assert time.monotonic() - started < 10
    assert lines(env) == ["startup"]


async def test_the_whole_process_group_is_killed_on_timeout(env, monkeypatch):
    monkeypatch.setenv("SANDBOX_STARTUP_TIMEOUT_SECONDS", "0.5")
    write_script(env, f"(sleep 1; echo orphan >> {env.log}) &\nsleep 30\n")
    sandboxd, tracker = children(env)
    with pytest.raises(RuntimeError):
        await supervisor.run(
            sandboxd_command=sandboxd, tracker_command=tracker, stop_event=asyncio.Event()
        )
    await asyncio.sleep(1.5)
    assert lines(env) == []


async def test_sigterm_while_the_script_runs_stops_the_supervisor(env):
    write_script(env, f"echo startup >> {env.log}; sleep 30; echo late >> {env.log}\n")
    sandboxd, tracker = children(env)
    stop = asyncio.Event()
    task = asyncio.create_task(
        supervisor.run(sandboxd_command=sandboxd, tracker_command=tracker, stop_event=stop)
    )
    deadline = time.monotonic() + 10
    while "startup" not in env.log.read_text():
        assert time.monotonic() < deadline
        await asyncio.sleep(0.02)
    stop.set()
    assert await asyncio.wait_for(task, 10) == 0
    assert lines(env) == ["startup"]  # no sandboxd, and the script did not finish
    assert not (env.root / ".session-runtime" / "startup-done").exists()


async def test_the_script_inherits_stdout_and_stderr(env, capfd):
    write_script(env, "echo to-stdout; echo to-stderr >&2\n")
    await run_until_children_start(env)
    out = capfd.readouterr()
    assert "to-stdout" in out.out and "to-stderr" in out.err
