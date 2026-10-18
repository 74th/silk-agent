"""Independent input, control, and turn loops for one conversation actor."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, TypeVar

from silk_agent.core.distributed_models import InputDeliveryState, InputEnvelope

log = logging.getLogger(__name__)
Prepared = TypeVar("Prepared")


class DispatchRetryable(Exception):
    """Raised by ``dispatch_input`` when the hand-off was refused without side effects."""


def _default_control_key(control: Any) -> str:
    if isinstance(control, Mapping):
        return str(control.get("input_id", id(control)))
    return str(getattr(control, "input_id", id(control)))


class ConversationDispatcher[Control, Prepared]:
    """Keep slow guards and SDK turns off the control reader's path.

    `prepare_input` may run concurrently, but durable dispatch is committed in
    input sequence order. `run_turns` owns its own long-lived task and should
    return only when stopped.
    """

    def __init__(
        self,
        *,
        read_inputs: Callable[[], Awaitable[Sequence[InputEnvelope]]],
        read_controls: Callable[[], Awaitable[Sequence[Control]]],
        prepare_input: Callable[[InputEnvelope], Awaitable[Prepared]],
        dispatch_input: Callable[[InputEnvelope, Prepared], Awaitable[None]],
        handle_control: Callable[[Control], Awaitable[None]],
        run_turns: Callable[[asyncio.Event], Awaitable[None]],
        dispatch_completion: Callable[[InputEnvelope], Awaitable[None]] | None = None,
        input_key: Callable[[InputEnvelope], str] = lambda item: item.input_id,
        control_key: Callable[[Control], str] = _default_control_key,
        poll_seconds: float = 1.0,
        read_limit: int | None = None,
        still_unfinished: Callable[[int], Awaitable[bool]] | None = None,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        # A full page may hide later records; absence from it is then not a cancellation.
        self._read_limit = read_limit
        # Asked only when the page is full: is the input with this seq still unfinished?
        self._still_unfinished = still_unfinished
        self._read_inputs = read_inputs
        self._read_controls = read_controls
        self._prepare_input = prepare_input
        self._dispatch_input = dispatch_input
        self._handle_control = handle_control
        self._run_turns = run_turns
        self._dispatch_completion = dispatch_completion
        self._input_key = input_key
        self._control_key = control_key
        self._poll_seconds = poll_seconds
        self._input_wake = asyncio.Event()
        self._control_wake = asyncio.Event()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._stop = asyncio.Event()
        self._sequence_changed = asyncio.Condition()
        self._pending_sequences: set[int] = set()
        self._seen_inputs: set[str] = set()
        self._seen_controls: set[str] = set()
        self._input_tasks: dict[str, tuple[asyncio.Task[Any], int]] = {}

    def wake_inputs(self) -> None:
        self._input_wake.set()

    def wake_controls(self) -> None:
        self._control_wake.set()

    async def run(self) -> None:
        loops = [
            asyncio.create_task(self._input_reader(), name="conversation-input-reader"),
            asyncio.create_task(self._control_reader(), name="conversation-control-reader"),
            asyncio.create_task(self._turn_runner(), name="conversation-turn-runner"),
        ]
        self._tasks.update(loops)
        try:
            await self._stop.wait()
        finally:
            self._stop.set()
            self._input_wake.set()
            self._control_wake.set()
            for task in tuple(self._tasks):
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

    def stop(self) -> None:
        self._stop.set()
        self._input_wake.set()
        self._control_wake.set()

    async def _input_reader(self) -> None:
        while not self._stop.is_set():
            try:
                records = sorted(await self._read_inputs(), key=lambda item: item.seq)
                active_keys = {self._input_key(record) for record in records}
                complete = self._read_limit is None or len(records) < self._read_limit
                if complete:
                    await self._drop_vanished(active_keys)
                else:
                    # A full page can hide valid inputs: absence alone proves nothing. Ask the
                    # store about each waiting input that is missing before cancelling it.
                    await self._drop_vanished(
                        active_keys, verify=self._still_unfinished, verified_only=True
                    )
                    self._seen_inputs.intersection_update(active_keys | set(self._input_tasks))
                for record in records:
                    if record.delivery_state not in {
                        InputDeliveryState.PENDING,
                        InputDeliveryState.QUEUED,
                    }:
                        continue
                    key = self._input_key(record)
                    if key in self._seen_inputs:
                        continue
                    self._seen_inputs.add(key)
                    self._pending_sequences.add(record.seq)
                    task = asyncio.create_task(self._prepare_and_dispatch(record, key))
                    self._input_tasks[key] = (task, record.seq)
                    task.add_done_callback(lambda _t, k=key: self._input_tasks.pop(k, None))
                    self._track(task)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("conversation input scan failed (%s)", type(exc).__name__)
            await self._wait(self._input_wake)

    async def _drop_vanished(
        self,
        active_keys: set[str],
        *,
        verify: Callable[[int], Awaitable[bool]] | None = None,
        verified_only: bool = False,
    ) -> None:
        """A waiting input that left the unfinished set was cancelled (stop barrier) or
        finished elsewhere: stop its task and release the sequence it was holding."""
        for key, (task, seq) in tuple(self._input_tasks.items()):
            if key in active_keys or task.done():
                continue
            if verified_only or verify is not None:
                if verify is None:
                    continue  # a full page and no way to check: keep waiting
                try:
                    if await verify(seq):
                        continue  # still unfinished, merely beyond this page
                except Exception as exc:
                    log.warning("could not verify a waiting input (%s)", type(exc).__name__)
                    continue
            task.cancel()
            async with self._sequence_changed:
                self._pending_sequences.discard(seq)
                self._sequence_changed.notify_all()
        self._seen_inputs.intersection_update(active_keys | set(self._input_tasks))

    async def _prepare_and_dispatch(self, record: InputEnvelope, key: str) -> None:
        committed = False
        try:
            if record.kind == "command_finished" and self._dispatch_completion is not None:
                async with self._sequence_changed:
                    await self._sequence_changed.wait_for(
                        lambda: (
                            self._stop.is_set()
                            or record.seq == min(self._pending_sequences, default=record.seq)
                        )
                    )
                if self._stop.is_set():
                    return
                await self._dispatch_completion(record)
                committed = True
                return
            # Preparation has durable side effects (guard history, conversation owner), and the
            # guard's context depends on earlier inputs: run it in acceptance order, never
            # speculatively. STOP and control handling stay on their own tasks.
            async with self._sequence_changed:
                await self._sequence_changed.wait_for(
                    lambda: (
                        self._stop.is_set()
                        or record.seq == min(self._pending_sequences, default=record.seq)
                    )
                )
            if self._stop.is_set():
                return
            prepared = await self._prepare_input(record)
            while True:
                try:
                    await self._dispatch_input(record, prepared)
                    break
                except DispatchRetryable:
                    # Preparation (guard, history, owner) is not repeated; only the hand-off.
                    await asyncio.sleep(self._poll_seconds)
                    if self._stop.is_set():
                        return
            committed = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("conversation input processing failed (%s)", type(exc).__name__)
        finally:
            async with self._sequence_changed:
                if committed:
                    self._pending_sequences.discard(record.seq)
                    self._sequence_changed.notify_all()
                else:
                    self._seen_inputs.discard(key)
                    # A failed or cancelled attempt must not keep later inputs waiting; the
                    # reader re-adds the sequence if the record is still unfinished.
                    self._pending_sequences.discard(record.seq)
                    self._sequence_changed.notify_all()
            if committed:
                self.wake_inputs()

    async def _control_reader(self) -> None:
        while not self._stop.is_set():
            try:
                controls = await self._read_controls()
                for control in controls:
                    key = self._control_key(control)
                    if key in self._seen_controls:
                        continue
                    self._seen_controls.add(key)
                    task = asyncio.create_task(self._handle_control_once(control, key))
                    self._track(task)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("conversation control scan failed (%s)", type(exc).__name__)
            await self._wait(self._control_wake)

    async def _handle_control_once(self, control: Control, key: str) -> None:
        try:
            await self._handle_control(control)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("conversation control processing failed (%s)", type(exc).__name__)
        finally:
            self._seen_controls.discard(key)
            self.wake_controls()

    async def _turn_runner(self) -> None:
        try:
            await self._run_turns(self._stop)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("conversation turn runner failed (%s)", type(exc).__name__)
        finally:
            self.stop()

    def _track(self, task: asyncio.Task[Any]) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _wait(self, event: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(event.wait(), timeout=self._poll_seconds)
        except TimeoutError:
            pass
        event.clear()
