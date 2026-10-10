"""LocalProcessRuntime: development-only runtime that runs commands on the host.

Each conversation gets its own temporary directory, but there is NO isolation: commands
run as the Bot's user. Never use it for the Slack bot in production (the Slack app refuses
to start with it unless explicitly allowed).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import itertools
import os
import shutil
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

from silk_agent.core.ports import CommandResult, FileEntry


@dataclass
class _Command:
    result: CommandResult
    output: str = ""
    process: asyncio.subprocess.Process | None = None


@dataclass
class _Conv:
    root: Path
    commands: dict[str, _Command] = field(default_factory=dict)
    running: set[str] = field(default_factory=set)


class LocalProcessRuntime:
    def __init__(self, base_dir: str | Path) -> None:
        self._base = Path(base_dir)
        self._convs: dict[str, _Conv] = {}
        self._ids = itertools.count(1)

    # ------------------------------------------------------------ paths

    def _conv(self, key: str) -> _Conv:
        conv = self._convs.get(key)
        if conv is None:
            root = self._base / hashlib.sha256(key.encode()).hexdigest()[:24]
            root.mkdir(parents=True, exist_ok=True)
            conv = self._convs[key] = _Conv(root.resolve())
        return conv

    def root(self, key: str) -> Path:
        return self._conv(key).root

    def _resolve(self, key: str, path: str) -> Path:
        """Map a sandbox path (absolute or relative) to the conversation's directory."""
        root = self._conv(key).root
        p = (root / path.lstrip("/")).resolve()
        if p != root and root not in p.parents:
            raise PermissionError(f"path escapes the workspace: {path}")
        return p

    # ------------------------------------------------------------ commands

    async def run_command(
        self,
        conversation_key: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout_seconds: float | None = None,
        on_started: Callable[[str], Awaitable[None]] | None = None,
    ) -> CommandResult:
        conv = self._conv(conversation_key)
        cid = f"local-{next(self._ids)}"
        workdir = self._resolve(conversation_key, cwd) if cwd else conv.root
        workdir.mkdir(parents=True, exist_ok=True)
        proc = await asyncio.create_subprocess_exec(
            "bash",
            "-lc",
            command,
            cwd=workdir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "HOME": str(conv.root)},
            start_new_session=True,
        )
        entry = _Command(CommandResult(cid, "running"), process=proc)
        conv.commands[cid] = entry
        conv.running.add(cid)
        if on_started is not None:
            await on_started(cid)
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout_seconds)
        except TimeoutError:
            await self._kill(proc)
            entry.result = CommandResult(cid, "cancelled", None, "", "timeout")
            return entry.result
        except asyncio.CancelledError:
            await self._kill(proc)
            raise
        finally:
            conv.running.discard(cid)
        stdout, stderr = out.decode(errors="replace"), err.decode(errors="replace")
        entry.output = stdout + stderr
        if entry.result.outcome == "cancelled":
            return entry.result
        entry.result = CommandResult(cid, "completed", proc.returncode, stdout, stderr)
        return entry.result

    @staticmethod
    async def _kill(proc: asyncio.subprocess.Process) -> None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, 9)
        with contextlib.suppress(Exception):
            await proc.wait()

    async def command_status(self, conversation_key: str, command_id: str) -> CommandResult:
        entry = self._conv(conversation_key).commands.get(command_id)
        return entry.result if entry else CommandResult(command_id, "unknown")

    async def command_output(
        self, conversation_key: str, command_id: str, cursor: int = 0
    ) -> tuple[str, int]:
        entry = self._conv(conversation_key).commands.get(command_id)
        out = entry.output if entry else ""
        return out[cursor:], len(out)

    async def cancel_commands(self, conversation_key: str) -> None:
        conv = self._conv(conversation_key)
        for cid in list(conv.running):
            entry = conv.commands[cid]
            if entry.process is not None:
                entry.result = CommandResult(cid, "cancelled")
                await self._kill(entry.process)

    # ------------------------------------------------------------ files

    async def read_file(self, conversation_key: str, path: str) -> bytes:
        return await asyncio.to_thread(self._resolve(conversation_key, path).read_bytes)

    async def write_file(self, conversation_key: str, path: str, content: bytes) -> None:
        target = self._resolve(conversation_key, path)

        def write() -> None:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)

        await asyncio.to_thread(write)

    async def list_files(self, conversation_key: str, path: str) -> list[FileEntry]:
        target = self._resolve(conversation_key, path)
        root = self._conv(conversation_key).root

        def scan() -> list[FileEntry]:
            if not target.is_dir():
                return []
            return [
                FileEntry("/" + str(p.relative_to(root)), p.is_dir(), p.stat().st_size)
                for p in sorted(target.iterdir())
            ]

        return await asyncio.to_thread(scan)

    # ------------------------------------------------------------ notices / skills

    async def pop_notices(self, conversation_key: str) -> list[str]:
        return []

    async def ack_notices(self, conversation_key: str) -> None:
        return None

    async def copy_skills(self, conversation_key: str, source: Path, generation: str) -> None:
        dest = self._conv(conversation_key).root / ".skills"

        def copy() -> None:
            if dest.exists():
                shutil.rmtree(dest)
            skills = source / ".claude" / "skills"
            if skills.is_dir():
                shutil.copytree(skills, dest)

        await asyncio.to_thread(copy)

    async def aclose(self) -> None:
        for key in list(self._convs):
            await self.cancel_commands(key)
