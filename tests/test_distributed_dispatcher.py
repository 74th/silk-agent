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
async def test_inputs_are_prepared_and_dispatched_strictly_in_sequence_order():
    first_entered = asyncio.Event()
    first_release = asyncio.Event()
    both_dispatched = asyncio.Event()
    prepared_order = []
    dispatched = []
    records = [input_envelope(2), input_envelope(1)]

    async def prepare(record):
        prepared_order.append(record.seq)
        if record.seq == 1:
            first_entered.set()
            await first_release.wait()
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
    await first_entered.wait()
    await asyncio.sleep(0.1)
    # The later input is not judged (guard, owner, history) while the earlier one is pending.
    assert prepared_order == [1] and dispatched == []
    first_release.set()
    await both_dispatched.wait()
    assert prepared_order == [1, 2] and dispatched == [1, 2]
    dispatcher.stop()
    await task


@pytest.mark.asyncio
async def test_stop_that_cancels_a_guard_pending_input_releases_the_inputs_behind_it():
    first_entered = asyncio.Event()
    dispatched = []
    records = [input_envelope(1)]
    never = asyncio.Event()

    async def prepare(record):
        if record.seq == 1:
            first_entered.set()
            await never.wait()  # the guard never answers
        return record.seq

    async def run_turns(stop):
        await stop.wait()

    async def dispatch(envelope, _prepared):
        dispatched.append(envelope.seq)

    dispatcher = ConversationDispatcher(
        read_inputs=lambda: _resolved(tuple(records)),
        read_controls=lambda: _resolved(()),
        prepare_input=prepare,
        dispatch_input=dispatch,
        handle_control=lambda _control: _resolved(None),
        run_turns=run_turns,
        poll_seconds=0.01,
        read_limit=100,
    )
    task = asyncio.create_task(dispatcher.run())
    await first_entered.wait()
    # STOP cancels input 1 durably (it leaves the unfinished set); input 3 arrives after it.
    records[:] = [input_envelope(3)]
    await asyncio.wait_for(_until(lambda: dispatched == [3]), 2)
    dispatcher.stop()
    await task


@pytest.mark.asyncio
async def test_a_full_page_does_not_cancel_waiting_inputs_that_may_be_on_the_next_page():
    first_entered = asyncio.Event()
    release = asyncio.Event()
    dispatched = []
    full_page = [input_envelope(seq) for seq in range(2, 5)]
    records = list(full_page)

    async def prepare(record):
        if record.seq == 2:
            first_entered.set()
            await release.wait()
        return record.seq

    async def run_turns(stop):
        await stop.wait()

    async def dispatch(envelope, _prepared):
        dispatched.append(envelope.seq)

    dispatcher = ConversationDispatcher(
        read_inputs=lambda: _resolved(tuple(records)),
        read_controls=lambda: _resolved(()),
        prepare_input=prepare,
        dispatch_input=dispatch,
        handle_control=lambda _control: _resolved(None),
        run_turns=run_turns,
        poll_seconds=0.01,
        read_limit=3,
    )
    task = asyncio.create_task(dispatcher.run())
    await first_entered.wait()
    records[:] = full_page[1:] + [input_envelope(5)]  # input 2 fell off the (full) page
    await asyncio.sleep(0.1)
    release.set()
    await asyncio.wait_for(_until(lambda: 2 in dispatched), 2)
    dispatcher.stop()
    await task


async def _until(predicate):
    while not predicate():  # noqa: ASYNC110
        await asyncio.sleep(0.01)


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


@pytest.mark.asyncio
async def test_a_refused_hand_off_is_retried_without_repeating_preparation():
    from silk_agent.distributed.dispatcher import DispatchRetryable

    prepared = []
    attempts = []
    done = asyncio.Event()

    async def prepare(record):
        prepared.append(record.seq)
        return record.seq

    async def run_turns(stop):
        await stop.wait()

    async def dispatch(envelope, _prepared):
        attempts.append(envelope.seq)
        if len(attempts) < 3:
            raise DispatchRetryable()  # refused with no side effects
        done.set()

    dispatcher = ConversationDispatcher(
        read_inputs=lambda: _resolved((input_envelope(1),)),
        read_controls=lambda: _resolved(()),
        prepare_input=prepare,
        dispatch_input=dispatch,
        handle_control=lambda _control: _resolved(None),
        run_turns=run_turns,
        poll_seconds=0.01,
    )
    task = asyncio.create_task(dispatcher.run())
    await asyncio.wait_for(done.wait(), 2)
    assert attempts == [1, 1, 1] and prepared == [1]  # guard/history ran once
    dispatcher.stop()
    await task


@pytest.mark.asyncio
@pytest.mark.parametrize(("still_unfinished", "expect_unblocked"), [(False, True), (True, False)])
async def test_full_page_cancellation_is_verified_per_waiting_input(
    still_unfinished, expect_unblocked
):
    first_entered = asyncio.Event()
    never = asyncio.Event()
    dispatched = []
    full_page = [input_envelope(seq) for seq in range(3, 6)]
    records = [input_envelope(1), *full_page[:2]]

    async def prepare(record):
        if record.seq == 1:
            first_entered.set()
            await never.wait()
        return record.seq

    async def run_turns(stop):
        await stop.wait()

    async def dispatch(envelope, _prepared):
        dispatched.append(envelope.seq)

    asked = []

    async def verify(seq):
        asked.append(seq)
        return still_unfinished

    dispatcher = ConversationDispatcher(
        read_inputs=lambda: _resolved(tuple(records)),
        read_controls=lambda: _resolved(()),
        prepare_input=prepare,
        dispatch_input=dispatch,
        handle_control=lambda _control: _resolved(None),
        run_turns=run_turns,
        poll_seconds=0.01,
        read_limit=3,
        still_unfinished=verify,
    )
    task = asyncio.create_task(dispatcher.run())
    await first_entered.wait()
    # STOP cancelled input 1; the next page is full (3 entries), so input 1 is merely absent.
    records[:] = full_page
    await asyncio.sleep(0.15)
    assert 1 in asked
    assert bool(dispatched) is expect_unblocked
    dispatcher.stop()
    await task
