"""Small in-container probes used by the GKE integration validation."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import socket
import sys
import time
import urllib.error
import urllib.request
import uuid


def _request(base: str, path: str, method: str = "GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=4) as response:
            return response.status, response.read(65536).decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(65536).decode()


def _wait(base: str, command_id: str, expected: str, seconds: float = 10) -> dict:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        status, payload = _request(base, f"/v1/commands/{command_id}")
        if status != 200:
            raise RuntimeError(f"tracker GET returned {status}: {payload}")
        record = json.loads(payload)
        if record["state"] in {expected, "unknown"}:
            if record["state"] != expected:
                raise RuntimeError(f"command {command_id} became unknown: {payload}")
            return record
        time.sleep(0.05)
    raise TimeoutError(f"command {command_id} did not reach {expected}")


def tracker(base: str, recovered_id: str = "") -> None:
    recovered = "none"
    if recovered_id:
        record = _wait(base, recovered_id, "finished")
        if record.get("stop_reason") != "tracker_restart" or "output may be incomplete" not in record.get("error", ""):
            raise RuntimeError(f"interrupted process was not safely recovered: {record}")
        recovered = "tracker_restart"
    marker = "/workspace/.session-runtime/network-probe-marker"
    req = {"command_id": "network-probe-check", "command": ["sh", "-c",
        f"printf x >> {marker}; printf 'probe-out\\n'; printf 'probe-err\\n' >&2; exit 7"],
        "env": {"TOKEN": "probe-secret"}, "check": True}
    status, body = _request(base, "/v1/commands", "POST", req)
    if status not in (200, 202) or "probe-secret" in body:
        raise RuntimeError(f"tracker first reservation returned {status}: {body}")
    duplicate_status, _ = _request(base, "/v1/commands", "POST", req)
    conflict_status, _ = _request(base, "/v1/commands", "POST", {"command_id": req["command_id"], "command": ["echo", "different"]})
    if duplicate_status != 200 or conflict_status != 409:
        raise RuntimeError(f"duplicate/conflict statuses were {duplicate_status}/{conflict_status}")
    result = _wait(base, req["command_id"], "finished")
    if (result.get("exit_code") != 7 or base64.b64decode(result.get("stdout", "")) != b"probe-out\n"
            or base64.b64decode(result.get("stderr", "")) != b"probe-err\n"
            or result.get("error") != "command exited with status 7"):
        raise RuntimeError(f"tracker did not retain command result: {result}")
    with open(marker, "rb") as stream:
        if stream.read() != b"x":
            raise RuntimeError("duplicate command ran more than once")

    stdin_id = f"network-probe-stdin-{uuid.uuid4().hex}"
    if _request(base, "/v1/commands", "POST", {"command_id": stdin_id, "command": ["cat"]})[0] != 202:
        raise RuntimeError("stdin command did not start")
    _wait(base, stdin_id, "running")
    if _request(base, f"/v1/commands/{stdin_id}/stdin", "POST", {"input": base64.b64encode(b"hello").decode()})[0] != 204:
        raise RuntimeError("stdin input was rejected")
    if _request(base, f"/v1/commands/{stdin_id}/stdin", "POST", {"eof": True})[0] != 204:
        raise RuntimeError("stdin EOF was rejected")
    if base64.b64decode(_wait(base, stdin_id, "finished").get("stdout", "")) != b"hello":
        raise RuntimeError("stdin output was not retained")

    pty_id = f"network-probe-pty-{uuid.uuid4().hex}"
    _request(base, "/v1/commands", "POST", {"command_id": pty_id, "command": ["sleep", "30"], "pty": True})
    _wait(base, pty_id, "running")
    if _request(base, f"/v1/commands/{pty_id}/resize", "POST", {"cols": 120, "rows": 40})[0] != 204:
        raise RuntimeError("PTY resize was rejected")
    if _request(base, f"/v1/commands/{pty_id}/signal", "POST", {"signal": "SIGINT"})[0] != 204:
        raise RuntimeError("signal was rejected")
    _wait(base, pty_id, "finished")

    timeout_id = f"network-probe-timeout-{uuid.uuid4().hex}"
    _request(base, "/v1/commands", "POST", {"command_id": timeout_id, "command": ["sleep", "60"], "timeout_seconds": 1})
    if _wait(base, timeout_id, "finished").get("stop_reason") != "timeout":
        raise RuntimeError("timeout was not recorded")

    cancel_id = f"network-probe-cancel-{uuid.uuid4().hex}"
    _request(base, "/v1/commands", "POST", {"command_id": cancel_id, "command": ["sleep", "60"]})
    _wait(base, cancel_id, "running")
    if _request(base, f"/v1/commands/{cancel_id}/cancel", "POST", {})[0] != 202:
        raise RuntimeError("cancel was rejected")
    if _wait(base, cancel_id, "finished").get("stop_reason") != "cancelled":
        raise RuntimeError("cancel was not recorded")

    descendant_id = f"network-probe-descendant-{uuid.uuid4().hex}"
    _request(base, "/v1/commands", "POST", {
        "command_id": descendant_id,
        "command": ["sh", "-c", "sleep 60 & exit 0"],
    })
    descendant = _wait(base, descendant_id, "running")
    time.sleep(0.2)
    descendant = json.loads(_request(base, f"/v1/commands/{descendant_id}")[1])
    if descendant["state"] != "running":
        raise RuntimeError(f"shell exit hid a surviving descendant: {descendant}")
    if _request(base, f"/v1/commands/{descendant_id}/cancel", "POST", {})[0] != 202:
        raise RuntimeError("could not cancel command with a surviving descendant")
    if _wait(base, descendant_id, "finished").get("stop_reason") != "cancelled":
        raise RuntimeError("surviving descendant was not terminated with its command")

    active_id = f"network-probe-active-{uuid.uuid4().hex}"
    _request(base, "/v1/commands", "POST", {"command_id": active_id, "command": ["sleep", "60"]})
    _wait(base, active_id, "running")
    gate_status, gate_body = _request(base, "/v1/commands/gate", "PUT", {"accepting": False})
    if gate_status != 200 or json.loads(gate_body).get("active_commands", 0) < 1:
        raise RuntimeError(f"gate close failed: {gate_status} {gate_body}")
    rejected_status, rejected_body = _request(base, "/v1/commands", "POST", {"command_id": "network-probe-after-gate", "command": ["true"]})
    if rejected_status != 423:
        raise RuntimeError(f"closed command gate expected HTTP 423, got {rejected_status}: {rejected_body}")
    if _request(base, "/v1/commands", "POST", req)[0] != 200:
        raise RuntimeError("closed gate rejected an idempotent retry")
    if _request(base, "/v1/commands/gate", "PUT", {"accepting": True})[0] != 200:
        raise RuntimeError("gate reopen failed")
    print(f"tracker-command-runtime-ok first={status} duplicate=200 conflict=409 exit=7 stdout-stderr=retained marker=once stdin-eof=ok pty-resize=ok signal=ok timeout=ok cancel=ok descendant=tracked-and-terminated gate=closed-and-reopened active={active_id} recovered={recovered}")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    for name in ("dns", "tcp", "http", "https"):
        command = sub.add_parser(name)
        command.add_argument("target")
        if name == "tcp":
            command.add_argument("port", type=int)
    command = sub.add_parser("tracker")
    command.add_argument("base_url")
    command.add_argument("recovered_id", nargs="?", default="")
    args = parser.parse_args()
    try:
        if args.mode == "dns":
            addresses = sorted({item[4][0] for item in socket.getaddrinfo(args.target, None)})
            print(f"dns-ok name={args.target} addresses={addresses}")
        elif args.mode == "tcp":
            with socket.create_connection((args.target, args.port), timeout=4):
                pass
            print(f"tcp-ok host={args.target} port={args.port}")
        elif args.mode in {"http", "https"}:
            with urllib.request.urlopen(args.target, timeout=4) as response:
                payload = response.read(4096).decode(errors="replace") if args.mode == "http" else ""
                print(f"{args.mode}-ok url={args.target} status={response.status} body={payload}")
        else:
            tracker(args.base_url, args.recovered_id)
    except Exception as exc:
        print(f"{args.mode} probe failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
