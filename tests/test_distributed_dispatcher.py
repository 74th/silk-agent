from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from silk_agent.core.distributed_models import InputEnvelope
from silk_agent.distributed.dispatcher import ConversationDispatcher


def input_envelope(seq):
    return InputEnvelope(
        conversation_key="conversation-a",
        seq=seq,
        input_id=f"input-{seq}",
        conversation_generation=1,
        kind="message",
        payload={"text": str(seq)},
        created_at=datetime(2026, 10, 14, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_control_reader_and_turn_runner_progress_while_input_guard_waits():
    guard_entered = asyncio.Event()
    guard_release = asyncio.Event()
    control_handled = asyncio.Event()
    turn_started = asyncio.Event()
    records = [input_envelope(1)]
    dispatched = []

    async def prepare(record):
        guard_entered.set()
        await guard_release.wait()
        return record.seq

    async def run_turns(stop):
        turn_started.set()
        await stop.wait()

    async def dispatch(envelope, _prepared):
        dispatched.append(envelope.seq)

    dispatcher = ConversationDispatcher(
        read_inputs=lambda: _resolved(tuple(records)),
        read_controls=lambda: _resolved(("stop",)),
        prepare_input=prepare,
        dispatch_input=dispatch,
        handle_control=lambda _control: _set(control_handled),
        run_turns=run_turns,
        poll_seconds=0.01,
    )
    task = asyncio.create_task(dispatcher.run())
    await asyncio.gather(guard_entered.wait(), control_handled.wait(), turn_started.wait())
    assert dispatched == []
    dispatcher.stop()
    guard_release.set()
    await task


@pytest.mark.asyncio
async def test_input_guard_completion_is_dispatched_in_sequence_order():
    first_entered = asyncio.Event()
    first_release = asyncio.Event()
    second_finished = asyncio.Event()
    both_dispatched = asyncio.Event()
    dispatched = []
    records = [input_envelope(2), input_envelope(1)]

    async def prepare(record):
        if record.seq == 1:
            first_entered.set()
            await first_release.wait()
        else:
            second_finished.set()
        return record.seq

    async def run_turns(stop):
        await stop.wait()

    async def dispatch(envelope, _prepared):
        dispatched.append(envelope.seq)
        if len(dispatched) == 2:
            both_dispatched.set()

    dispatcher = ConversationDispatcher(
        read_inputs=lambda: _resolved(tuple(records)),
        read_controls=lambda: _resolved(()),
        prepare_input=prepare,
        dispatch_input=dispatch,
        handle_control=lambda _control: _resolved(None),
        run_turns=run_turns,
        poll_seconds=0.01,
    )
    task = asyncio.create_task(dispatcher.run())
    await asyncio.gather(first_entered.wait(), second_finished.wait())
    assert dispatched == []
    first_release.set()
    await both_dispatched.wait()
    assert dispatched == [1, 2]
    dispatcher.stop()
    await task


@pytest.mark.asyncio
async def test_completion_input_uses_notification_dispatcher():
    completion = InputEnvelope(
        conversation_key="conversation-a",
        seq=1,
        input_id="completion",
        conversation_generation=1,
        kind="command_finished",
        payload={"command_id": "cmd"},
        created_at=datetime(2026, 10, 14, tzinfo=UTC),
    )
    prepared = []
    completed = []
    both_done = asyncio.Event()

    async def prepare(record):
        prepared.append(record)
        return record

    async def dispatch(_record, _prepared):
        raise AssertionError("completion must use its notification path")

    async def dispatch_completion(record):
        completed.append(record.input_id)
        both_done.set()

    dispatcher = ConversationDispatcher(
        read_inputs=lambda: _resolved((completion,) if not completed else ()),
        read_controls=lambda: _resolved(()),
        prepare_input=prepare,
        dispatch_input=dispatch,
        handle_control=lambda _control: _resolved(None),
        run_turns=lambda stop: stop.wait(),
        dispatch_completion=dispatch_completion,
        poll_seconds=0.01,
    )
    task = asyncio.create_task(dispatcher.run())
    await both_done.wait()
    dispatcher.stop()
    await task
    assert completed == ["completion"]
    assert prepared == []


async def _resolved(value):
    return value


async def _set(event):
    event.set()
