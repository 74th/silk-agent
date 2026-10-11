"""Own sandboxd and its Python command tracker as sibling processes."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
from collections.abc import Sequence
from pathlib import Path

import aiohttp

STARTUP_SCRIPT_ENV = "SANDBOX_STARTUP_SCRIPT"
DEFAULT_STARTUP_SCRIPT = "/opt/session-runtime/startup.sh"
STARTUP_TIMEOUT_ENV = "SANDBOX_STARTUP_TIMEOUT_SECONDS"
DEFAULT_STARTUP_TIMEOUT_SECONDS = 300.0
STARTUP_ON_ERROR_ENV = "SANDBOX_STARTUP_ON_ERROR"  # "fail" (default) or "continue"
STARTUP_FIRST_RUN_ENV = "SANDBOX_STARTUP_FIRST_RUN"  # given to the script: "1" or "0"
STARTUP_DONE_MARKER = Path(".session-runtime") / "startup-done"


class StartupFailed(RuntimeError):
    pass


async def _kill_group(process: asyncio.subprocess.Process, grace: float = 5) -> None:
    """SIGTERM the script's process group, then SIGKILL it after `grace` seconds."""
    for sig, wait in ((signal.SIGTERM, grace), (signal.SIGKILL, None)):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, sig)
        try:
            await asyncio.wait_for(process.wait(), wait)
            return
        except TimeoutError:
            continue


def _startup_plan() -> tuple[str, Path, Path, bool, list[str]] | None:
    script = os.environ.get(STARTUP_SCRIPT_ENV, DEFAULT_STARTUP_SCRIPT)
    if not os.path.isfile(script):
        return None
    root = Path(os.environ.get("SANDBOXD_ROOT_DIR", "/workspace"))
    marker = root / STARTUP_DONE_MARKER
    command = [script] if os.access(script, os.X_OK) else ["/bin/sh", script]
    return script, root, marker, not marker.exists(), command


def _mark_startup_done(marker: Path) -> None:
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()


async def _run_startup(stopping: asyncio.Event) -> bool:
    """Run the use case's startup script, if the image has one, before sandboxd starts.

    Returns False when a stop was requested while it ran (the caller exits). A failure raises
    `StartupFailed` unless SANDBOX_STARTUP_ON_ERROR=continue."""
    plan = _startup_plan()
    if plan is None:
        return True
    script, root, marker, first_run, command = plan
    timeout = float(os.environ.get(STARTUP_TIMEOUT_ENV, DEFAULT_STARTUP_TIMEOUT_SECONDS))
    on_error = os.environ.get(STARTUP_ON_ERROR_ENV, "fail")
    env = {**os.environ, STARTUP_FIRST_RUN_ENV: "1" if first_run else "0"}
    print(f"session supervisor: running {script} (first run: {first_run})", file=sys.stderr)

    failure: str | None = None
    process = await asyncio.create_subprocess_exec(
        *command, cwd=root, env=env, start_new_session=True
    )
    exited = asyncio.ensure_future(process.wait())
    stop = asyncio.ensure_future(stopping.wait())
    try:
        await asyncio.wait({exited, stop}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        if not exited.done():
            await _kill_group(process)
            if stop.done():
                print(
                    "session supervisor: stopped while running the startup script", file=sys.stderr
                )
                return False
            failure = f"startup script {script} did not finish within {timeout:g} seconds"
        elif process.returncode != 0:
            failure = f"startup script {script} failed with exit code {process.returncode}"
    finally:
        for task in (exited, stop):
            task.cancel()
        await asyncio.gather(exited, stop, return_exceptions=True)
    if failure is None:
        _mark_startup_done(marker)
        return True
    print(f"session supervisor: {failure}", file=sys.stderr)
    if on_error == "continue":
        print("session supervisor: continuing (SANDBOX_STARTUP_ON_ERROR=continue)", file=sys.stderr)
        return True
    raise StartupFailed(failure)


async def _wait_ready(process: asyncio.subprocess.Process, limit_seconds: float = 45) -> None:
    deadline = asyncio.get_running_loop().time() + limit_seconds
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.5)) as session:
        while asyncio.get_running_loop().time() < deadline:
            if process.returncode is not None:
                raise RuntimeError(
                    f"sandboxd exited before readiness (status {process.returncode})"
                )
            try:
                async with session.get("http://127.0.0.1:8080/v1/health") as response:
                    if response.status == 200:
                        return
            except (TimeoutError, aiohttp.ClientError):
                pass
            await asyncio.sleep(0.1)
    raise TimeoutError(f"sandboxd did not become ready within {limit_seconds:g} seconds")


async def _stop(process: asyncio.subprocess.Process | None, grace: float = 5) -> None:
    if process is None or process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), grace)
    except TimeoutError:
        process.kill()
        await process.wait()


async def run(
    *,
    sandboxd_command: Sequence[str] | None = None,
    tracker_command: Sequence[str] | None = None,
    stop_event: asyncio.Event | None = None,
) -> int:
    """Run both children; stopping tracker first closes active sandboxd streams."""
    sandboxd_command = sandboxd_command or (
        os.environ.get("SANDBOXD_BINARY", "/usr/local/bin/sandboxd"),
        "--listen-host=0.0.0.0",
        f"--root-dir={os.environ.get('SANDBOXD_ROOT_DIR', '/workspace')}",
    )
    tracker_command = tracker_command or (sys.executable, "-m", "gke_session_sandbox.tracker")
    stopping = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals = []
    if stop_event is None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stopping.set)
            installed_signals.append(sig)

    sandboxd = tracker = stop_wait = sandboxd_wait = tracker_wait = None
    try:
        if not await _run_startup(stopping):
            return 0
        sandboxd = await asyncio.create_subprocess_exec(*sandboxd_command)
        await _wait_ready(sandboxd)
        tracker = await asyncio.create_subprocess_exec(*tracker_command)
        stop_wait = asyncio.create_task(stopping.wait())
        sandboxd_wait = asyncio.create_task(sandboxd.wait())
        tracker_wait = asyncio.create_task(tracker.wait())
        done, _ = await asyncio.wait(
            (stop_wait, sandboxd_wait, tracker_wait), return_when=asyncio.FIRST_COMPLETED
        )
        if stop_wait in done:
            return 0
        if tracker.returncode is not None:
            raise RuntimeError(
                f"tracker exited ({tracker.returncode}); stopping sandboxd closes active command "
                f"streams"
            )
        raise RuntimeError(f"sandboxd exited ({sandboxd.returncode}); tracker cannot continue")
    finally:
        # Cancel tracker first so its gRPC Start streams close before sandboxd exits.
        await _stop(tracker)
        await _stop(sandboxd)
        for task in (stop_wait, sandboxd_wait, tracker_wait):
            if task is not None and not task.done():
                task.cancel()
        for sig in installed_signals:
            loop.remove_signal_handler(sig)


async def serve() -> None:
    status = await run()
    if status:
        raise SystemExit(status)


def main() -> None:
    try:
        asyncio.run(serve())
    except (RuntimeError, TimeoutError) as exc:  # StartupFailed is a RuntimeError
        print(f"session supervisor: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
