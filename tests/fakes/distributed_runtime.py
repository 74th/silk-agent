from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

from silk_agent.core.events import TurnFinished
from silk_agent.core.ports import CommandResult, TurnRequest


class FakeClock:
    """A manually advanced clock whose sleepers wake only when tests advance time."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=UTC)
        self._sleepers: list[tuple[datetime, asyncio.Future[None]]] = []

    def now(self) -> datetime:
        return self._now

    async def sleep(self, seconds: float) -> None:
        target = self._now + timedelta(seconds=seconds)
        if target <= self._now:
            return
        future = asyncio.get_running_loop().create_future()
        self._sleepers.append((target, future))
        await future

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)
        remaining: list[tuple[datetime, asyncio.Future[None]]] = []
        for target, future in self._sleepers:
            if target <= self._now:
                if not future.done():
                    future.set_result(None)
            else:
                remaining.append((target, future))
        self._sleepers = remaining


class AsyncBarrier:
    """One-shot gate exposing an explicit point at which an operation is paused."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self._released = asyncio.Event()

    async def wait(self) -> None:
        self.entered.set()
        await self._released.wait()

    async def wait_until_entered(self) -> None:
        await self.entered.wait()

    def release(self) -> None:
        self._released.set()


class FakeSdk:
    """Controllable SDK double for two-worker actor tests."""

    def __init__(self, gate: AsyncBarrier | None = None) -> None:
        self.gate = gate
        self.requests: list[TurnRequest] = []
        self.appended: list[str] = []
        self.interrupt_count = 0
        self.closed = False

    async def run_turn(self, request: TurnRequest) -> AsyncIterator[Any]:
        self.requests.append(request)
        if self.gate is not None:
            await self.gate.wait()
        yield TurnFinished(
            conversation_key=request.conversation_key,
            turn_id=request.turn_id,
            result="fake result",
        )

    async def interrupt(self) -> None:
        self.interrupt_count += 1
        if self.gate is not None:
            self.gate.release()

    async def append(self, text: str) -> bool:
        self.appended.append(text)
        return True

    async def reset(self) -> None:
        self.requests.clear()
        self.appended.clear()

    async def close(self) -> None:
        self.closed = True


class FakeRemoteCommand:
    """Stable-ID command fake; detaching a waiter never cancels the command."""

    def __init__(self, gate: AsyncBarrier | None = None) -> None:
        self.gate = gate
        self.commands: dict[str, str] = {}
        self.start_calls: list[tuple[str, str]] = []
        self.cancel_calls: list[str] = []
        self.completed: set[str] = set()

    async def start(self, command_id: str, command: str) -> str:
        existing = self.commands.get(command_id)
        if existing is not None and existing != command:
            raise ValueError("command_id payload conflict")
        self.commands[command_id] = command
        self.start_calls.append((command_id, command))
        return command_id

    async def wait(self, command_id: str) -> CommandResult:
        if command_id not in self.commands:
            return CommandResult(command_id, "unknown")
        if self.gate is not None:
            await self.gate.wait()
        if command_id in self.completed:
            return CommandResult(command_id, "completed", exit_code=0, stdout="done")
        return CommandResult(command_id, "running")

    async def cancel(self, command_id: str) -> None:
        self.cancel_calls.append(command_id)

    def finish(self, command_id: str) -> None:
        self.completed.add(command_id)
        if self.gate is not None:
            self.gate.release()
