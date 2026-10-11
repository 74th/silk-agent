"""Python CommandTracker for the sandboxd ProcessService."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import signal
import tempfile
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import grpc
from aiohttp import web
from google.protobuf.empty_pb2 import Empty

from ._sandboxd_proto import process_pb2, process_pb2_grpc
from .errors import CommandOutputLimitExceeded, OutputExpiredError, OutputStorageLimitExceeded, OutputWriteError

_COMMAND_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_TRACKER_MARKER_ENV = "GKE_SESSION_SANDBOX_TRACKER_MARKER"
_SIGNALS = {
    "SIGINT": process_pb2.SIGNAL_SIGINT,
    "SIGTERM": process_pb2.SIGNAL_SIGTERM,
    "SIGKILL": process_pb2.SIGNAL_SIGKILL,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class CommandJournal:
    """Atomic JSON journal persisted on the session workspace volume."""

    def __init__(self, path: Path, *, command_output_limit_bytes: int = 64 * 1024 * 1024,
                 session_output_limit_bytes: int = 256 * 1024 * 1024,
                 buffered_response_limit_bytes: int = 8 * 1024 * 1024,
                 output_retention_seconds: int = 604800,
                 clock: Callable[[], datetime] | None = None):
        if min(command_output_limit_bytes, session_output_limit_bytes,
               buffered_response_limit_bytes, output_retention_seconds) <= 0:
            raise ValueError("output limits and retention must be positive")
        self.path = path
        self.output_dir = path.parent / "command-output"
        self.command_output_limit_bytes = command_output_limit_bytes
        self.session_output_limit_bytes = session_output_limit_bytes
        self.buffered_response_limit_bytes = buffered_response_limit_bytes
        self.output_retention_seconds = output_retention_seconds
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.lock = asyncio.Lock()
        self.accepting = True
        self.commands: dict[str, dict[str, Any]] = {}
        if path.exists():
            saved = json.loads(path.read_text())
            if not isinstance(saved.get("commands"), dict) or not isinstance(saved.get("accepting"), bool):
                raise ValueError("command journal has an invalid shape")
            self.accepting = saved["accepting"]
            self.commands = saved["commands"]

    def _persist(self) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(prefix="commands.", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w") as output:
                json.dump({"accepting": self.accepting, "commands": self.commands}, output, separators=(",", ":"))
                output.flush()
                os.fsync(output.fileno())
            os.chmod(temp_path, 0o600)
            os.replace(temp_path, self.path)
            dir_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    async def reserve(self, request: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        command_id = request.get("command_id")
        command = request.get("command")
        if not isinstance(command_id, str) or not _COMMAND_ID.fullmatch(command_id):
            raise ValueError("command_id must contain 1-128 letters, digits, '.', '_' or '-' and start with a letter or digit")
        if not isinstance(command, list) or not command or not all(isinstance(arg, str) for arg in command) or not command[0]:
            raise ValueError("command must be a nonempty argv list")
        env = request.get("env", {})
        if not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
            raise ValueError("env must be a string map")
        if _TRACKER_MARKER_ENV in env:
            raise ValueError(f"env key {_TRACKER_MARKER_ENV} is reserved by the tracker")
        cwd = request.get("cwd", "")
        timeout = request.get("timeout_seconds", 0)
        pty = request.get("pty", False)
        cols, rows = request.get("pty_cols", 80), request.get("pty_rows", 24)
        check = request.get("check", False)
        if not isinstance(cwd, str) or not isinstance(timeout, int) or isinstance(timeout, bool) or not 0 <= timeout <= 86400:
            raise ValueError("invalid cwd or timeout_seconds")
        if not isinstance(pty, bool) or not isinstance(check, bool):
            raise ValueError("pty and check must be boolean")
        if not isinstance(cols, int) or not 1 <= cols <= 65535 or not isinstance(rows, int) or not 1 <= rows <= 65535:
            raise ValueError("PTY dimensions must be positive integers")
        normalized = {
            "command_id": command_id,
            "command": command,
            "env": env,
            "cwd": cwd,
            "check": check,
            "timeout_seconds": timeout,
            "pty": pty,
            "pty_cols": cols,
            "pty_rows": rows,
        }
        async with self.lock:
            previous = self.commands.get(command_id)
            if previous:
                previous_request = {key: previous.get(key, default) for key, default in normalized.items()}
                if previous_request != normalized:
                    raise Conflict("command ID already belongs to a different request")
                return previous.copy(), False
            if not self.accepting:
                raise GateClosed("command admission gate is closed")
            record = {
                **normalized, "state": "reserved", "reserved_at": _now(),
                "stdout": "", "stderr": "", "stdout_bytes": 0, "stderr_bytes": 0,
                "stdout_truncated": False, "stderr_truncated": False,
                "output_complete": True, "output_expired": False,
            }
            self.commands[command_id] = record
            try:
                self._persist()
            except Exception:
                self.commands.pop(command_id, None)
                raise
            return record.copy(), True

    async def update(self, command_id: str, **values: Any) -> dict[str, Any]:
        async with self.lock:
            record = self.commands[command_id]
            old = record.copy()
            record.update(values)
            try:
                self._persist()
            except Exception:
                self.commands[command_id] = old
                raise
            return record.copy()

    async def append_output(self, command_id: str, key: str, chunk: bytes) -> None:
        if key not in {"stdout", "stderr"}:
            raise ValueError("output stream must be stdout or stderr")
        if not chunk:
            return
        async with self.lock:
            record = self.commands[command_id]
            if record.get("output_expired"):
                raise OutputExpiredError(command_id)
            command_total = record.get("stdout_bytes", 0) + record.get("stderr_bytes", 0)
            session_total = sum(
                item.get("stdout_bytes", len(base64.b64decode(item.get("stdout", ""))))
                + item.get("stderr_bytes", len(base64.b64decode(item.get("stderr", ""))))
                for item in self.commands.values() if not item.get("output_expired")
            )
            command_remaining = max(0, self.command_output_limit_bytes - command_total)
            session_remaining = max(0, self.session_output_limit_bytes - session_total)
            accepted_bytes = min(len(chunk), command_remaining, session_remaining)
            limit_error: Exception | None = None
            if accepted_bytes < len(chunk):
                missing = len(chunk) - accepted_bytes
                if command_remaining <= session_remaining:
                    self._mark_output_error(record, "command_output_limit", "command output limit exceeded",
                                            missing, self.command_output_limit_bytes)
                    limit_error = CommandOutputLimitExceeded(command_id, self.command_output_limit_bytes)
                else:
                    self._mark_output_error(record, "session_output_limit", "session output storage limit exceeded",
                                            missing, self.session_output_limit_bytes)
                    limit_error = OutputStorageLimitExceeded(command_id, self.session_output_limit_bytes)
            self.output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            output_path = self._output_path(command_id, key)
            prior_file_size = output_path.stat().st_size if output_path.exists() else 0
            old = record.copy()
            try:
                if accepted_bytes:
                    with output_path.open("ab") as output:
                        output.write(chunk[:accepted_bytes])
                        output.flush()
                        os.fsync(output.fileno())
                record[f"{key}_bytes"] = record.get(f"{key}_bytes", 0) + accepted_bytes
                record[f"{key}_truncated"] = record[f"{key}_bytes"] > self.buffered_response_limit_bytes
                self._persist()
            except OSError as exc:
                self.commands[command_id] = old
                try:
                    with output_path.open("r+b") as output:
                        output.truncate(prior_file_size)
                except OSError:
                    pass
                self._mark_output_error(self.commands[command_id], "output_storage_error", "output storage write failed", len(chunk))
                raise OutputWriteError(command_id) from exc
            except Exception:
                self.commands[command_id] = old
                try:
                    with output_path.open("r+b") as output:
                        output.truncate(prior_file_size)
                except OSError:
                    pass
                raise
            if limit_error is not None:
                raise limit_error

    @staticmethod
    def _mark_output_error(record: dict[str, Any], code: str, message: str, missing: int, limit: int | None = None) -> None:
        record.update(output_complete=False, output_error_code=code, output_error=message,
                      output_missing_bytes=record.get("output_missing_bytes", 0) + missing,
                      output_error_limit_bytes=limit)

    def _output_path(self, command_id: str, stream: str) -> Path:
        if not _COMMAND_ID.fullmatch(command_id):
            raise ValueError("invalid command_id")
        return self.output_dir / f"{command_id}.{stream}"

    async def set_gate(self, accepting: bool) -> int:
        async with self.lock:
            previous = self.accepting
            self.accepting = accepting
            try:
                self._persist()
            except Exception:
                self.accepting = previous
                raise
            return sum(record.get("state") != "finished" for record in self.commands.values())

    async def get(self, command_id: str) -> dict[str, Any] | None:
        async with self.lock:
            record = self.commands.get(command_id)
            if record is None:
                return None
            previous = record.copy()
            if self._expire_output_if_needed(record):
                try:
                    self._persist()
                except Exception:
                    self.commands[command_id] = previous
                    raise
                self._remove_output_files(command_id)
            return record.copy()

    async def read_output(self, command_id: str, stream: str, cursor: int, limit: int) -> dict[str, Any]:
        if stream not in {"stdout", "stderr"} or cursor < 0 or not 1 <= limit <= 1024 * 1024:
            raise ValueError("invalid output stream, cursor, or limit")
        async with self.lock:
            record = self.commands.get(command_id)
            if record is None:
                raise KeyError(command_id)
            previous = record.copy()
            if self._expire_output_if_needed(record):
                try:
                    self._persist()
                except Exception:
                    self.commands[command_id] = previous
                    raise
                self._remove_output_files(command_id)
            if record.get("output_expired"):
                raise OutputExpiredError(command_id)
            total = record.get(f"{stream}_bytes", len(base64.b64decode(record.get(stream, ""))))
            path = self._output_path(command_id, stream)
            if path.exists():
                with path.open("rb") as output:
                    output.seek(cursor)
                    data = output.read(min(limit, max(0, total - cursor)))
            else:
                data = base64.b64decode(record.get(stream, ""))[cursor:cursor + limit]
            next_cursor = cursor + len(data)
            return {"command_id": command_id, "stream": stream, "data": base64.b64encode(data).decode("ascii"),
                    "cursor": next_cursor, "total_bytes": total, "complete": next_cursor >= total,
                    "output_complete": record.get("output_complete", True)}

    async def read_buffered_output(self, command_id: str, stream: str) -> bytes:
        async with self.lock:
            record = self.commands[command_id]
            if record.get("output_expired"):
                raise OutputExpiredError(command_id)
            path = self._output_path(command_id, stream)
            if not path.exists():
                return b""
            with path.open("rb") as output:
                return output.read(self.buffered_response_limit_bytes)

    def _expire_output_if_needed(self, record: dict[str, Any]) -> bool:
        finished = record.get("finished_at")
        if record.get("output_expired") or not finished:
            return False
        try:
            finished_at = datetime.fromisoformat(finished)
        except (TypeError, ValueError):
            return False
        expiry = finished_at + timedelta(seconds=self.output_retention_seconds)
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("output expiry clock must return a timezone-aware datetime")
        if now < expiry:
            return False
        for stream in ("stdout", "stderr"):
            record[stream] = ""
        record.update(output_expired=True, output_expired_at=_now())
        return True

    def _remove_output_files(self, command_id: str) -> None:
        for stream in ("stdout", "stderr"):
            self._output_path(command_id, stream).unlink(missing_ok=True)

    async def expire_outputs(self) -> int:
        async with self.lock:
            expired: list[str] = []
            previous: dict[str, dict[str, Any]] = {}
            for command_id, record in self.commands.items():
                old_record = record.copy()
                if self._expire_output_if_needed(record):
                    previous[command_id] = old_record
                    expired.append(command_id)
            if expired:
                try:
                    self._persist()
                except Exception:
                    self.commands.update(previous)
                    raise
                for command_id in expired:
                    self._remove_output_files(command_id)
            return len(expired)

    async def recover(self, group_present) -> None:
        async with self.lock:
            previous = json.loads(json.dumps(self.commands))
            now = _now()
            for record in self.commands.values():
                if record.get("state") not in {"reserved", "running", "unknown"}:
                    continue
                identity = record.get("identity")
                if not identity:
                    record.update(state="unknown", error="tracker restarted before process identity was recorded")
                    continue
                try:
                    observation = await group_present(identity["pgid"])
                    present, detail = observation if isinstance(observation, tuple) else (observation, "")
                except Exception:
                    present, detail = True, "process group inspection failed"
                if present:
                    suffix = f": {detail}" if detail else ""
                    record.update(state="unknown", error=f"process group may still be present after tracker restart{suffix}")
                else:
                    record.update(
                        state="finished",
                        finished_at=now,
                        exit_code=None,
                        stop_reason="tracker_restart",
                        error="tracker restarted after process group ended; exit status is unknown and output may be incomplete",
                    )
            try:
                self._persist()
            except Exception:
                self.commands = previous
                raise


class Conflict(Exception):
    pass


class GateClosed(Exception):
    pass


class HTTPGateClosed(web.HTTPClientError):
    status_code = 423
    reason = "Locked"


def _public(record: dict[str, Any]) -> dict[str, Any]:
    result = {key: value for key, value in record.items() if key not in {"env", "identity"}}
    if record.get("state") != "finished":
        result.pop("stdout", None)
        result.pop("stderr", None)
    return result


class Tracker:
    def __init__(self, journal: CommandJournal, stub, generation: int):
        self.journal = journal
        self.stub = stub
        self.generation = generation
        self.tasks: dict[str, asyncio.Task] = {}

    async def start(self) -> None:
        health = await self.stub.Execute(process_pb2.ExecuteRequest(config=process_pb2.ProcessConfig(command=["/bin/true"])), timeout=3)
        if health.exit_code != 0:
            raise RuntimeError("sandboxd process API did not pass readiness command")

    async def health(self, _request: web.Request) -> web.Response:
        try:
            result = await self.stub.Execute(
                process_pb2.ExecuteRequest(config=process_pb2.ProcessConfig(command=["/bin/true"])), timeout=2
            )
        except grpc.aio.AioRpcError as exc:
            raise web.HTTPServiceUnavailable(text="sandboxd unavailable") from exc
        if result.exit_code != 0:
            raise web.HTTPServiceUnavailable(text="sandboxd unavailable")
        return web.json_response({"status": "ok", "tracker_generation": self.generation, "sandboxd": "connected"})

    async def gate(self, request: web.Request) -> web.Response:
        body = await request.json()
        if not isinstance(body, dict) or not isinstance(body.get("accepting"), bool):
            raise web.HTTPBadRequest(text="gate request must contain accepting")
        active = await self.journal.set_gate(body["accepting"])
        return web.json_response({"accepting": body["accepting"], "active_commands": active})

    async def create(self, request: web.Request) -> web.Response:
        body = await request.json()
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="command request must be an object")
        try:
            record, created = await self.journal.reserve(body)
        except Conflict as exc:
            raise web.HTTPConflict(text=str(exc)) from exc
        except GateClosed as exc:
            raise HTTPGateClosed(text=str(exc)) from exc
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        except OSError as exc:
            raise web.HTTPInternalServerError(text="could not persist command journal") from exc
        if created:
            self.tasks[record["command_id"]] = asyncio.create_task(self.execute(record["command_id"]))
        return web.json_response(_public(record), status=202 if created else 200)

    async def get(self, request: web.Request) -> web.Response:
        command_id = request.match_info["command_id"]
        record = await self.journal.get(command_id)
        if record is None:
            raise web.HTTPNotFound()
        public = _public(record)
        if record.get("state") == "finished" and not record.get("output_expired"):
            for stream in ("stdout", "stderr"):
                public[stream] = base64.b64encode(await self.journal.read_buffered_output(command_id, stream)).decode("ascii")
        return web.json_response(public)

    async def output(self, request: web.Request) -> web.Response:
        command_id = request.match_info["command_id"]
        stream = request.query.get("stream", "stdout")
        try:
            cursor = int(request.query.get("cursor", "0"))
            limit = int(request.query.get("limit", str(64 * 1024)))
            page = await self.journal.read_output(command_id, stream, cursor, limit)
        except KeyError as exc:
            raise web.HTTPNotFound() from exc
        except OutputExpiredError as exc:
            raise web.HTTPGone(text=str(exc)) from exc
        except (ValueError, TypeError) as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        return web.json_response(page)

    async def execute(self, command_id: str) -> None:
        record = await self.journal.get(command_id)
        if not record:
            return
        marker = uuid.uuid4().hex
        process_env = dict(record["env"])
        process_env[_TRACKER_MARKER_ENV] = marker
        config = process_pb2.ProcessConfig(command=record["command"], env_vars=process_env)
        if record["cwd"]:
            config.cwd = record["cwd"]
        start = process_pb2.StartRequest(config=config)
        if record["pty"]:
            start.pty.CopyFrom(process_pb2.PTY(cols=record["pty_cols"], rows=record["pty_rows"]))
        call = self.stub.Start(start)
        stream_task = asyncio.create_task(self._consume(command_id, call, marker))
        timeout = record["timeout_seconds"] or 120
        try:
            await asyncio.wait_for(asyncio.shield(stream_task), timeout=timeout)
        except TimeoutError:
            await self.stop(command_id, "timeout")
            try:
                await asyncio.wait_for(asyncio.shield(stream_task), timeout=5)
            except TimeoutError:
                current = await self.journal.get(command_id)
                if current and current.get("process_id"):
                    await self._send_signal(current["process_id"], process_pb2.SIGNAL_SIGKILL)
                try:
                    await asyncio.wait_for(asyncio.shield(stream_task), timeout=5)
                except TimeoutError:
                    stream_task.cancel()
                    await self._mark_unknown(command_id, "process did not exit after SIGKILL")
        except asyncio.CancelledError:
            call.cancel()
            await self._mark_unknown(command_id, "tracker stopped while command was running")
            raise
        except Exception as exc:
            await self._mark_unknown(command_id, str(exc))
        finally:
            self.tasks.pop(command_id, None)

    async def _consume(self, command_id: str, call, marker: str) -> None:
        output_failure: Exception | None = None
        async for event in call:
            kind = event.WhichOneof("event")
            if kind == "init":
                pid = event.init.process_id
                identity = await self._read_identity(pid, marker)
                values: dict[str, Any] = {"state": "running", "process_id": pid, "started_at": _now()}
                if identity:
                    values["identity"] = identity
                else:
                    values["error"] = "process identity could not be read"
                await self.journal.update(command_id, **values)
            elif kind == "stdout":
                if output_failure is None:
                    try:
                        await self.journal.append_output(command_id, "stdout", event.stdout)
                    except (CommandOutputLimitExceeded, OutputStorageLimitExceeded, OutputWriteError, OSError) as exc:
                        output_failure = exc
                        await self._handle_output_failure(command_id, exc)
            elif kind == "stderr":
                if output_failure is None:
                    try:
                        await self.journal.append_output(command_id, "stderr", event.stderr)
                    except (CommandOutputLimitExceeded, OutputStorageLimitExceeded, OutputWriteError, OSError) as exc:
                        output_failure = exc
                        await self._handle_output_failure(command_id, exc)
            elif kind == "exit":
                code = event.exit.exit_code
                record = await self.journal.get(command_id)
                values = {"state": "finished", "exit_code": code, "finished_at": _now(),
                          "output_complete": output_failure is None and (record or {}).get("output_complete", True)}
                if output_failure is not None:
                    values["error"] = str(output_failure)
                elif record and record.get("check") and code:
                    values["error"] = f"command exited with status {code}"
                await self.journal.update(command_id, **values)
                return
        current = await self.journal.get(command_id)
        if current and current.get("state") not in {"finished", "unknown"}:
            await self._mark_unknown(command_id, "sandboxd Start stream ended without an exit event")

    async def _handle_output_failure(self, command_id: str, error: Exception) -> None:
        record = await self.journal.get(command_id)
        if record is not None:
            if isinstance(error, CommandOutputLimitExceeded):
                code = "command_output_limit"
            elif isinstance(error, OutputStorageLimitExceeded):
                code = "session_output_limit"
            else:
                code = "output_storage_error"
            try:
                await self.journal.update(command_id, output_complete=False,
                                          output_error_code=code, output_error=str(error),
                                          output_error_limit_bytes=getattr(error, "limit_bytes", None))
            except OSError:
                pass
            try:
                await self.stop(command_id, "output_storage_error")
            except grpc.aio.AioRpcError:
                pass

    async def _read_identity(self, process_id: int, marker: str) -> dict[str, Any] | None:
        try:
            scan = (
                "for f in /proc/[0-9]*/environ; do "
                f"if tr '\\000' '\\n' < \"$f\" 2>/dev/null | grep -Fxq '{_TRACKER_MARKER_ENV}={marker}'; then "
                "p=${f#/proc/}; p=${p%/environ}; printf '%s ' \"$p\"; cat \"/proc/$p/stat\"; fi; done"
            )
            stat = await self.stub.Execute(
                process_pb2.ExecuteRequest(config=process_pb2.ProcessConfig(command=["/bin/sh", "-c", scan])), timeout=3
            )
            boot = await self.stub.Execute(process_pb2.ExecuteRequest(config=process_pb2.ProcessConfig(command=["cat", "/proc/sys/kernel/random/boot_id"])), timeout=2)
            if stat.exit_code or boot.exit_code:
                return None
            lines = stat.stdout.decode().splitlines()
            if len(lines) != 1:
                return None
            os_pid_text, stat_text = lines[0].split(" ", 1)
            end = stat_text.rfind(")")
            fields = stat_text[end + 1 :].split()
            if end < 0 or len(fields) <= 19:
                return None
            return {
                "process_id": process_id,
                "pid": int(os_pid_text),
                "pgid": int(fields[2]),
                "start_time": int(fields[19]),
                "boot_id": boot.stdout.decode().strip(),
            }
        except (grpc.aio.AioRpcError, ValueError, IndexError):
            return None

    async def _group_present(self, pgid: int) -> tuple[bool, str]:
        # sandboxd starts each Execute command in its own process group. If this
        # inspection shell was assigned the old PGID after PID reuse, its ps/awk
        # children would otherwise make the stale group appear to still exist.
        command = (
            f"[ \"$$\" -ne {pgid} ] || exit 1; "
            f"ps -eo pid=,pgid=,comm= | awk '$2 == {pgid} {{ print; found=1 }} END {{ exit !found }}'"
        )
        result = await self.stub.Execute(
            process_pb2.ExecuteRequest(config=process_pb2.ProcessConfig(command=["/bin/sh", "-c", command])), timeout=3
        )
        if result.exit_code == 0:
            return True, result.stdout.decode(errors="replace").strip()
        if result.exit_code == 1:
            return False, ""
        raise RuntimeError(f"could not inspect process group {pgid}")

    async def _send_signal(self, pid: int, value: int) -> None:
        await self.stub.SendSignal(process_pb2.SendSignalRequest(process_id=pid, signal=value), timeout=3)

    async def stop(self, command_id: str, reason: str) -> None:
        record = await self.journal.get(command_id)
        if not record or record.get("state") != "running" or not record.get("process_id"):
            return
        await self._send_signal(record["process_id"], process_pb2.SIGNAL_SIGTERM)
        await self.journal.update(command_id, stop_reason=reason)
        asyncio.create_task(self._escalate_kill(command_id))

    async def _escalate_kill(self, command_id: str) -> None:
        await asyncio.sleep(5)
        record = await self.journal.get(command_id)
        if record and record.get("state") == "running" and record.get("process_id"):
            try:
                await self._send_signal(record["process_id"], process_pb2.SIGNAL_SIGKILL)
            except grpc.aio.AioRpcError:
                pass

    async def stdin(self, request: web.Request) -> web.Response:
        record = await self._running(request)
        body = await request.json()
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="stdin request must be an object")
        has_input, eof = "input" in body, body.get("eof") is True
        if has_input == eof or (has_input and not isinstance(body["input"], str)):
            raise web.HTTPBadRequest(text="stdin request must contain input or eof=true")
        payload = process_pb2.WriteStdinRequest(process_id=record["process_id"])
        if eof:
            payload.eof.CopyFrom(Empty())
        else:
            try:
                payload.input = base64.b64decode(body["input"], validate=True)
            except ValueError as exc:
                raise web.HTTPBadRequest(text="input must be base64 encoded") from exc
        await self.stub.WriteStdin(payload, timeout=3)
        raise web.HTTPNoContent()

    async def send_signal(self, request: web.Request) -> web.Response:
        record = await self._running(request)
        body = await request.json()
        name = body.get("signal") if isinstance(body, dict) else None
        if name not in _SIGNALS:
            raise web.HTTPBadRequest(text="signal must be SIGINT, SIGTERM or SIGKILL")
        await self._send_signal(record["process_id"], _SIGNALS[name])
        raise web.HTTPNoContent()

    async def resize(self, request: web.Request) -> web.Response:
        record = await self._running(request)
        body = await request.json()
        cols, rows = body.get("cols"), body.get("rows")
        if (not isinstance(cols, int) or isinstance(cols, bool) or not isinstance(rows, int)
                or isinstance(rows, bool) or cols <= 0 or rows <= 0 or cols > 65535 or rows > 65535):
            raise web.HTTPBadRequest(text="resize requires positive cols and rows")
        await self.stub.ResizeTTY(process_pb2.ResizeTTYRequest(process_id=record["process_id"], cols=cols, rows=rows), timeout=3)
        raise web.HTTPNoContent()

    async def cancel(self, request: web.Request) -> web.Response:
        await self._running(request)
        await self.stop(request.match_info["command_id"], "cancelled")
        raise web.HTTPAccepted()

    async def _running(self, request: web.Request) -> dict[str, Any]:
        record = await self.journal.get(request.match_info["command_id"])
        if not record:
            raise web.HTTPNotFound()
        if record.get("state") != "running" or not record.get("process_id"):
            raise web.HTTPConflict(text="command is not running")
        return record

    async def _mark_unknown(self, command_id: str, error: str) -> None:
        record = await self.journal.get(command_id)
        if record and record.get("state") != "finished":
            await self.journal.update(command_id, state="unknown", error=error)


async def _generation(path: Path) -> int:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        previous = int(path.read_text())
    except FileNotFoundError:
        previous = 0
    if previous < 0:
        raise ValueError("tracker generation must not be negative")
    current = previous + 1
    fd, temporary = tempfile.mkstemp(prefix="generation.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as output:
            output.write(str(current))
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return current


async def serve() -> None:
    state_dir = Path(os.environ.get("SESSION_RUNTIME_DIR", "/workspace/.session-runtime"))
    generation = await _generation(state_dir / "tracker-generation")
    channel = grpc.aio.insecure_channel("127.0.0.1:9090")
    stub = process_pb2_grpc.ProcessServiceStub(channel)
    journal = CommandJournal(
        state_dir / "commands.json",
        command_output_limit_bytes=int(os.environ.get("SESSION_SANDBOX_COMMAND_OUTPUT_LIMIT_BYTES", 64 * 1024 * 1024)),
        session_output_limit_bytes=int(os.environ.get("SESSION_SANDBOX_SESSION_OUTPUT_LIMIT_BYTES", 256 * 1024 * 1024)),
        buffered_response_limit_bytes=int(os.environ.get("SESSION_SANDBOX_BUFFERED_RESPONSE_LIMIT_BYTES", 8 * 1024 * 1024)),
        output_retention_seconds=int(os.environ.get("SESSION_SANDBOX_OUTPUT_RETENTION_SECONDS", 604800)),
    )
    tracker = Tracker(journal, stub, generation)
    await tracker.start()
    if generation > 1:
        await tracker.journal.recover(tracker._group_present)
    app = web.Application(client_max_size=1024 * 1024)
    app.router.add_get("/healthz", tracker.health)
    app.router.add_put("/v1/commands/gate", tracker.gate)
    app.router.add_post("/v1/commands", tracker.create)
    app.router.add_get("/v1/commands/{command_id}", tracker.get)
    app.router.add_get("/v1/commands/{command_id}/output", tracker.output)
    app.router.add_post("/v1/commands/{command_id}/stdin", tracker.stdin)
    app.router.add_post("/v1/commands/{command_id}/signal", tracker.send_signal)
    app.router.add_post("/v1/commands/{command_id}/resize", tracker.resize)
    app.router.add_post("/v1/commands/{command_id}/cancel", tracker.cancel)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8081)
    await site.start()
    expiry_task = asyncio.create_task(_expire_outputs(journal))
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for handled_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(handled_signal, stopped.set)
    try:
        await stopped.wait()
    finally:
        expiry_task.cancel()
        await asyncio.gather(expiry_task, return_exceptions=True)
        for task in tuple(tracker.tasks.values()):
            task.cancel()
        await asyncio.gather(*tracker.tasks.values(), return_exceptions=True)
        await runner.cleanup()
        await channel.close()


async def _expire_outputs(journal: CommandJournal) -> None:
    interval = max(1, min(60, journal.output_retention_seconds))
    while True:
        await asyncio.sleep(interval)
        await journal.expire_outputs()


def main() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    main()
