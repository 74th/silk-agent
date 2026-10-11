"""Management-network fixture that checks REST and sandboxd gRPC access."""

from __future__ import annotations

import asyncio
import os
import socket
import time
import urllib.request

import grpc

from ._sandboxd_proto import process_pb2, process_pb2_grpc


async def _execute(stub, command: list[str]):
    return await stub.Execute(
        process_pb2.ExecuteRequest(config=process_pb2.ProcessConfig(command=command)), timeout=5
    )


async def _identity_probe(host: str) -> None:
    async with grpc.aio.insecure_channel(f"{host}:9090") as channel:
        stub = process_pb2_grpc.ProcessServiceStub(channel)
        marker = f"session-sandbox-probe-{time.time_ns()}"
        stream = stub.Start(process_pb2.StartRequest(config=process_pb2.ProcessConfig(
            command=["/bin/sh", "-c", "exec /bin/sleep 120"],
            env_vars={"SESSION_SANDBOX_PROBE": marker},
        )))
        event = await stream.read()
        if event.WhichOneof("event") != "init":
            raise RuntimeError(f"expected process init event, got {event.WhichOneof('event')}")
        scan = "for f in /proc/[0-9]*/environ; do if tr '\\000' '\\n' < \"$f\" 2>/dev/null | grep -Fxq 'SESSION_SANDBOX_PROBE=" + marker + "'; then p=${f#/proc/}; p=${p%/environ}; printf '%s ' \"$p\"; cat \"/proc/$p/stat\"; fi; done"
        result = await _execute(stub, ["/bin/sh", "-c", scan])
        if result.exit_code != 0 or marker not in result.stdout.decode(errors="replace"):
            raise RuntimeError("could not find the started process in sandboxd /proc")
        line = result.stdout.decode().strip().splitlines()[0]
        stat = line[line.rfind(")") + 1:].split()
        if len(stat) < 20 or int(stat[2]) != event.init.process_id:
            raise RuntimeError(f"sandboxd process identity was unexpected: pid={event.init.process_id} stat={line}")
        boot = await _execute(stub, ["/bin/cat", "/proc/sys/kernel/random/boot_id"])
        if boot.exit_code != 0 or not boot.stdout.strip():
            raise RuntimeError("could not read sandbox boot ID")
        stream.cancel()
        try:
            await stream.read()
        except grpc.aio.AioRpcError:
            pass
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            result = await _execute(stub, ["/bin/sh", "-c", scan])
            if result.exit_code == 0 and marker.encode() not in result.stdout:
                return
            await asyncio.sleep(0.1)
        raise RuntimeError(f"process remained after its Start stream disconnected (pid={event.init.process_id})")


async def _target(host: str) -> None:
    expected = b"written-by-rest-read-by-grpc\n"
    request = urllib.request.Request(f"http://{host}:8080/v1/files/management-runner.txt",
        data=expected, method="PUT")
    def write_file():
        with urllib.request.urlopen(request, timeout=15) as response:
            if response.status != 204:
                raise RuntimeError(f"REST PUT returned {response.status}, expected 204")
    await asyncio.to_thread(write_file)
    async with grpc.aio.insecure_channel(f"{host}:9090") as channel:
        try:
            response = await _execute(process_pb2_grpc.ProcessServiceStub(channel), ["cat", "/workspace/management-runner.txt"])
        except grpc.aio.AioRpcError as exc:
            raise RuntimeError(f"gRPC Execute: {exc}") from exc
        if response.exit_code != 0 or response.stdout != expected:
            raise RuntimeError(f"gRPC read returned exit={response.exit_code} stdout={response.stdout!r}")
    print(f"rest-write-and-grpc-read-ok target={host}")


async def main() -> None:
    targets = [part.strip() for part in os.environ.get("SANDBOX_TARGETS", "").split(",")]
    if len(targets) != 2 or not all(targets):
        raise RuntimeError("SANDBOX_TARGETS must contain service DNS and pod IP")
    for target in targets:
        await _target(target)
    await _identity_probe(targets[0])
    print(f"gvisor-process-identity-and-stream-disconnect-ok target={targets[0]}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"smoke runner failed: {exc}", file=__import__("sys").stderr)
        raise SystemExit(1) from exc
