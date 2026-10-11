"""Own sandboxd and its Python command tracker as sibling processes."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections.abc import Sequence

import aiohttp


async def _wait_ready(process: asyncio.subprocess.Process, timeout: float = 45) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.5)) as session:
        while asyncio.get_running_loop().time() < deadline:
            if process.returncode is not None:
                raise RuntimeError(f"sandboxd exited before readiness (status {process.returncode})")
            try:
                async with session.get("http://127.0.0.1:8080/v1/health") as response:
                    if response.status == 200:
                        return
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
            await asyncio.sleep(0.1)
    raise TimeoutError(f"sandboxd did not become ready within {timeout:g} seconds")


async def _stop(process: asyncio.subprocess.Process | None, grace: float = 5) -> None:
    if process is None or process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), grace)
    except TimeoutError:
        process.kill()
        await process.wait()


async def run(*, sandboxd_command: Sequence[str] | None = None,
              tracker_command: Sequence[str] | None = None,
              stop_event: asyncio.Event | None = None) -> int:
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

    sandboxd = tracker = None
    try:
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
            raise RuntimeError(f"tracker exited ({tracker.returncode}); stopping sandboxd closes active command streams")
        raise RuntimeError(f"sandboxd exited ({sandboxd.returncode}); tracker cannot continue")
    finally:
        # Cancel tracker first so its gRPC Start streams close before sandboxd exits.
        await _stop(tracker)
        await _stop(sandboxd)
        for task in locals().get("stop_wait",), locals().get("sandboxd_wait"), locals().get("tracker_wait"):
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
    except (RuntimeError, TimeoutError) as exc:
        print(f"session supervisor: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
