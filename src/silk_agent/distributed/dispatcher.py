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
        refresh_input: Callable[[InputEnvelope], Awaitable[InputEnvelope | None]] | None = None,
        abandon_input: Callable[[InputEnvelope], Awaitable[None]] | None = None,
        max_prepare_attempts: int = 12,
        max_dispatch_attempts: int = 60,
        retry_cap_seconds: float = 30.0,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        # A full page may hide later records; absence from it is then not a cancellation.
        self._read_limit = read_limit
        # Asked only when the page is full: is the input with this seq still unfinished?
        self._still_unfinished = still_unfinished
        self._refresh_input = refresh_input
        self._abandon_input = abandon_input
        self._max_prepare_attempts = max_prepare_attempts
        self._max_dispatch_attempts = max_dispatch_attempts
        self._retry_cap_seconds = retry_cap_seconds
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
        self._control_failures: dict[str, int] = {}
        self._input_tasks: dict[str, tuple[asyncio.Task[Any], int]] = {}
        self._delivering: set[str] = set()

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
                await self._drop_vanished(active_keys, complete=complete)
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

    async def _drop_vanished(self, active_keys: set[str], *, complete: bool) -> None:
        """Stop the task of a waiting input that was cancelled (stop barrier) or finished
        elsewhere, and release the sequence it was holding.

        Absence from the reader's page is not proof: the reader lists only deliverable inputs
        (pending/queued), so an input that moved on to dispatching is absent too, and a full page
        can hide later ones. A task that is already handing its input over is never cancelled; for
        the rest the store is asked whether the input is still unfinished in any state."""
        for key, (task, seq) in tuple(self._input_tasks.items()):
            if key in active_keys or task.done() or key in self._delivering:
                continue
            if self._still_unfinished is not None:
                try:
                    if await self._still_unfinished(seq):
                        continue
                except Exception as exc:
                    log.warning("could not verify a waiting input (%s)", type(exc).__name__)
                    continue
            elif not complete:
                continue  # a full page and no way to check: keep waiting
            task.cancel()
            async with self._sequence_changed:
                self._pending_sequences.discard(seq)
                self._sequence_changed.notify_all()
        self._seen_inputs.intersection_update(active_keys | set(self._input_tasks))

    async def _wait_for_turn(self, record: InputEnvelope) -> bool:
        """Wait until ``record`` is the oldest input still pending (False if stopped)."""
        async with self._sequence_changed:
            await self._sequence_changed.wait_for(
                lambda: (
                    self._stop.is_set()
                    or record.seq == min(self._pending_sequences, default=record.seq)
                )
            )
        return not self._stop.is_set()

    async def _retry_pause(self, attempt: int) -> bool:
        """Back off before a retry (capped). False when stopped."""
        delay = min(self._poll_seconds * (2**attempt), self._retry_cap_seconds)
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except TimeoutError:
            return True
        return False

    async def _latest(self, record: InputEnvelope) -> InputEnvelope | None:
        """The input as stored now, or None if it is no longer unfinished."""
        if self._refresh_input is None:
            return record
        return await self._refresh_input(record)

    async def _prepare_and_dispatch(self, record: InputEnvelope, key: str) -> None:
        committed = False
        try:
            if not await self._wait_for_turn(record):
                return
            if record.kind == "command_finished" and self._dispatch_completion is not None:
                await self._dispatch_completion(record)
                committed = True
                return
            # Preparation has durable side effects (guard history, conversation owner), and the
            # guard's context depends on earlier inputs: run it in acceptance order, never
            # speculatively. STOP and control handling stay on their own tasks.
            prepared = None
            attempt = 0
            while True:
                try:
                    prepared = await self._prepare_input(record)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # A transient failure must not let a later input overtake this one: keep
                    # the turn and retry with backoff. A poison input is abandoned after a
                    # bounded number of tries so it cannot hold the conversation forever.
                    attempt += 1
                    log.warning(
                        "input preparation failed (%s); attempt %d", type(exc).__name__, attempt
                    )
                    if attempt >= self._max_prepare_attempts:
                        if self._abandon_input is not None:
                            # Only a recorded abandonment frees the turn; if the write fails,
                            # keep the turn (later inputs must not overtake) and retry it.
                            while True:
                                try:
                                    await self._abandon_input(record)
                                    break
                                except asyncio.CancelledError:
                                    raise
                                except Exception as abandon_exc:
                                    log.warning(
                                        "could not abandon an input (%s)",
                                        type(abandon_exc).__name__,
                                    )
                                    if not await self._retry_pause(attempt):
                                        return
                        committed = True  # settled: release the turn to later inputs
                        return
                    if not await self._retry_pause(attempt):
                        return
                    latest = await self._latest(record)
                    if latest is None:
                        return  # cancelled (stop barrier) while waiting to retry
                    record = latest
            attempt = 0
            self._delivering.add(key)  # from here the reader must not cancel this task
            while True:
                try:
                    await self._dispatch_input(record, prepared)
                    break
                except DispatchRetryable:
                    # Preparation (guard, history, owner) is not repeated; only the hand-off,
                    # against the state stored now and for a bounded number of tries.
                    attempt += 1
                    if attempt >= self._max_dispatch_attempts:
                        log.warning("input dispatch kept being refused; giving up for now")
                        return  # the reader offers it again on a later scan
                    if not await self._retry_pause(min(attempt, 4)):
                        return
                    latest = await self._latest(record)
                    if latest is None:
                        return  # no longer unfinished
                    record = latest
            committed = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("conversation input processing failed (%s)", type(exc).__name__)
        finally:
            self._delivering.discard(key)
            async with self._sequence_changed:
                if committed:
                    self._pending_sequences.discard(record.seq)
                    self._sequence_changed.notify_all()
                else:
                    self._seen_inputs.discard(key)
                    # A cancelled or abandoned attempt must not keep later inputs waiting; the
                    # reader re-adds the sequence if the record is still unfinished.
                    self._pending_sequences.discard(record.seq)
                    self._sequence_changed.notify_all()
            if committed:
                self.wake_inputs()

    def forget_inputs(self, input_ids: list[str]) -> None:
        """Offer these inputs again: they left an in-memory queue but are still queued durably."""
        for input_id in input_ids:
            self._seen_inputs.discard(input_id)
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
        failed = False
        try:
            await self._handle_control(control)
            self._control_failures.pop(key, None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failed = True
            log.warning("conversation control processing failed (%s)", type(exc).__name__)
        try:
            if failed:
                # Do not wake the reader: it would re-read the same control at once and spin.
                # Keep the key marked as seen for a capped, growing pause, then let the next
                # poll offer it again.
                attempt = self._control_failures.get(key, 0)
                self._control_failures[key] = attempt + 1
                await self._retry_pause(attempt)
        finally:
            self._seen_controls.discard(key)
            if not failed:
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
