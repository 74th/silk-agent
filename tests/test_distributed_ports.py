import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.core.distributed_models import (
    CommandExecutionState,
    InputDeliveryState,
    InputEnvelope,
    OwnershipToken,
    WorkerRecord,
)
from silk_agent.core.ownership_lifecycle import OwnershipLifecycle
from tests.fakes.distributed_inbox import FakeInboxStore, inbox_store_contract
from tests.fakes.distributed_runtime import AsyncBarrier, FakeClock, FakeRemoteCommand, FakeSdk
from tests.fakes.distributed_store import (
    FakeSharedStore,
    as_command_store,
    as_inbox_store,
    as_outbox_store,
    as_ownership_store,
    as_worker_directory,
)

NOW = datetime(2026, 10, 14, 3, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_inbox_assigns_one_order_across_inputs_and_controls():
    inbox = inbox_store_contract(FakeInboxStore())
    first = await inbox.accept_input(
        conversation_key="conversation",
        input_id="input-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "first"},
        created_at=NOW,
    )
    control = await inbox.accept_control(
        conversation_key="conversation",
        input_id="control-1",
        conversation_generation=1,
        kind="stop",
        sender_id="user-1",
        payload={},
        created_at=NOW,
    )

    assert (first.seq, control.seq) == (1, 2)


@pytest.mark.asyncio
async def test_inbox_compare_and_set_requires_current_owner_and_expected_state():
    store = FakeInboxStore()
    inbox = inbox_store_contract(store)
    token = OwnershipToken(
        "conversation", "worker-1", "incarnation-1", 4, 1, NOW + timedelta(seconds=60)
    )
    store.owners[token.conversation_key] = token
    await inbox.accept_input(
        conversation_key="conversation",
        input_id="input-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "hello"},
        created_at=NOW,
    )

    assert await inbox.advance_input(
        conversation_key="conversation",
        input_id="input-1",
        owner=token,
        expected=InputDeliveryState.PENDING,
        new_state=InputDeliveryState.DISPATCHING,
        dispatch_attempt_id="attempt-1",
    )
    assert not await inbox.advance_input(
        conversation_key="conversation",
        input_id="input-1",
        owner=token,
        expected=InputDeliveryState.PENDING,
        new_state=InputDeliveryState.DISPATCHING,
    )
    assert not await inbox.advance_input(
        conversation_key="conversation",
        input_id="input-1",
        owner=OwnershipToken(
            "conversation", "worker-1", "incarnation-1", 3, 1, NOW + timedelta(seconds=60)
        ),
        expected=InputDeliveryState.DISPATCHING,
        new_state=InputDeliveryState.ACCEPTED,
    )
    saved = (await inbox.list_unfinished("conversation"))[0]
    assert saved.dispatch_attempt_id == "attempt-1"


@pytest.mark.asyncio
async def test_input_retry_requires_confirmed_unsent_and_uncertain_attempt_is_not_requeued():
    store = FakeInboxStore()
    inbox = inbox_store_contract(store)
    old_owner = OwnershipToken(
        "conversation", "worker-1", "incarnation-1", 4, 1, NOW + timedelta(seconds=60)
    )
    store.owners[old_owner.conversation_key] = old_owner
    await inbox.accept_input(
        conversation_key="conversation",
        input_id="input-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "hello"},
        created_at=NOW,
    )
    assert await inbox.advance_input(
        conversation_key="conversation",
        input_id="input-1",
        owner=old_owner,
        expected=InputDeliveryState.PENDING,
        new_state=InputDeliveryState.DISPATCHING,
        dispatch_attempt_id="attempt-1",
    )
    assert await inbox.advance_input(
        conversation_key="conversation",
        input_id="input-1",
        owner=old_owner,
        expected=InputDeliveryState.DISPATCHING,
        new_state=InputDeliveryState.PENDING,
    )
    assert await inbox.advance_input(
        conversation_key="conversation",
        input_id="input-1",
        owner=old_owner,
        expected=InputDeliveryState.PENDING,
        new_state=InputDeliveryState.DISPATCHING,
        dispatch_attempt_id="attempt-2",
    )

    new_owner = OwnershipToken(
        "conversation", "worker-2", "incarnation-2", 5, 1, NOW + timedelta(seconds=60)
    )
    store.owners[old_owner.conversation_key] = new_owner
    assert await inbox.recover_dispatching(
        conversation_key="conversation",
        input_id="input-1",
        previous_owner_epoch=old_owner.epoch,
        dispatch_attempt_id="attempt-2",
    )
    assert (await inbox.list_unfinished("conversation")) == ()


@pytest.mark.asyncio
async def test_fake_clock_wakes_waiter_only_after_explicit_advance():
    clock = FakeClock(NOW)
    sleeper = asyncio.create_task(clock.sleep(20))
    await asyncio.sleep(0)
    assert not sleeper.done()

    clock.advance(19)
    assert not sleeper.done()
    clock.advance(1)
    await sleeper
    assert clock.now() == NOW + timedelta(seconds=20)


@pytest.mark.asyncio
async def test_fake_sdk_and_remote_command_use_barriers_without_timing_sleeps():
    from silk_agent.core.ports import TurnRequest

    sdk_barrier = AsyncBarrier()
    sdk = FakeSdk(sdk_barrier)
    request = TurnRequest("conversation", "turn-1", "hello")
    turn = asyncio.create_task(collect(sdk.run_turn(request)))
    await sdk_barrier.wait_until_entered()
    assert sdk.requests == [request]
    sdk_barrier.release()
    await turn

    command_barrier = AsyncBarrier()
    command = FakeRemoteCommand(command_barrier)
    await command.start("command-1", "sleep 1")
    waiter = asyncio.create_task(command.wait("command-1"))
    await command_barrier.wait_until_entered()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert command.commands["command-1"] == "sleep 1"
    assert command.start_calls == [("command-1", "sleep 1")]
    assert command.cancel_calls == []


@pytest.mark.asyncio
async def test_shared_memory_store_is_visible_through_multiple_worker_ports():
    store = FakeSharedStore()
    gateway_view = as_inbox_store(store)
    worker_view = as_inbox_store(store)
    ownership_view = as_ownership_store(store)
    token = await ownership_view.acquire(
        conversation_key="conversation",
        worker_id="worker-1",
        worker_incarnation="incarnation-1",
        conversation_generation=1,
        now=NOW,
        lease_expires_at=NOW + timedelta(seconds=60),
    )
    assert token is not None

    await gateway_view.accept_input(
        conversation_key="conversation",
        input_id="input-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "hello"},
        created_at=NOW,
    )

    assert [item.input_id for item in await worker_view.list_unfinished("conversation")] == [
        "input-1"
    ]


@pytest.mark.asyncio
async def test_ownership_lifecycle_fences_expired_and_replaced_tokens():
    store = FakeSharedStore()
    ownership = as_ownership_store(store)
    assigned = await ownership.acquire(
        conversation_key="conversation",
        worker_id="worker-1",
        worker_incarnation="incarnation-1",
        conversation_generation=1,
        now=NOW,
        lease_expires_at=NOW + timedelta(seconds=60),
    )
    assert assigned is not None
    assert await ownership.activate(assigned, now=NOW + timedelta(seconds=1))
    renewed = await ownership.renew(
        assigned,
        now=NOW + timedelta(seconds=1),
        lease_expires_at=NOW + timedelta(seconds=120),
    )
    assert renewed is not None
    assert renewed.epoch == assigned.epoch
    assert (
        await ownership.renew(
            assigned,
            now=NOW + timedelta(seconds=1),
            lease_expires_at=NOW + timedelta(seconds=180),
        )
        is None
    )
    assert (
        await ownership.renew(
            renewed,
            now=renewed.expires_at,
            lease_expires_at=NOW + timedelta(seconds=240),
        )
        is None
    )
    assert not await ownership.release(assigned, now=NOW + timedelta(seconds=1))
    assert await ownership.release(renewed, now=NOW + timedelta(seconds=1))
    assert await ownership.get("conversation") is None


@pytest.mark.asyncio
async def test_idle_ownership_closes_actor_before_releasing_and_keeps_lease_on_close_error():
    events: list[str] = []

    class OrderedStore(FakeSharedStore):
        async def release(self, token: OwnershipToken, *, now: datetime) -> bool:
            events.append("release")
            return await super().release(token, now=now)

    store = OrderedStore()
    ownership = as_ownership_store(store)
    token = await ownership.acquire(
        conversation_key="conversation",
        worker_id="worker-1",
        worker_incarnation="incarnation-1",
        conversation_generation=1,
        now=NOW,
        lease_expires_at=NOW + timedelta(seconds=60),
    )
    assert token is not None
    assert await ownership.activate(token, now=NOW + timedelta(seconds=1))
    lifecycle = OwnershipLifecycle(ownership, FakeClock(NOW + timedelta(seconds=1)))

    async def save_and_close() -> None:
        events.append("close")

    assert await lifecycle.release_when_idle(
        token,
        is_idle=lambda: True,
        save_and_close=save_and_close,
    )
    assert events == ["close", "release"]
    assert await ownership.get("conversation") is None

    token = await ownership.acquire(
        conversation_key="next-conversation",
        worker_id="worker-1",
        worker_incarnation="incarnation-1",
        conversation_generation=1,
        now=NOW,
        lease_expires_at=NOW + timedelta(seconds=60),
    )
    assert token is not None
    assert await ownership.activate(token, now=NOW + timedelta(seconds=1))

    async def fail_to_close() -> None:
        events.append("close-failed")
        raise RuntimeError("save failed")

    with pytest.raises(RuntimeError, match="save failed"):
        await lifecycle.release_when_idle(
            token,
            is_idle=lambda: True,
            save_and_close=fail_to_close,
        )
    assert events == ["close", "release", "close-failed"]
    assert await ownership.get("next-conversation") == token


@pytest.mark.asyncio
async def test_command_terminal_and_completion_input_are_committed_together():
    store = FakeSharedStore()
    inbox = as_inbox_store(store)
    commands = as_command_store(store)
    token = await as_ownership_store(store).acquire(
        conversation_key="conversation",
        worker_id="worker-1",
        worker_incarnation="incarnation-1",
        conversation_generation=1,
        now=NOW,
        lease_expires_at=NOW + timedelta(seconds=60),
    )
    assert token is not None
    original = await inbox.accept_input(
        conversation_key="conversation",
        input_id="input-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "run command"},
        created_at=NOW,
    )
    assert await commands.create_intent(
        command_id="command-1",
        request_hash="hash-1",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=2,
        origin_turn="turn-1",
        origin_epoch=token.epoch,
        origin_seq=original.seq,
        owner=token,
        created_at=NOW,
    )
    assert await commands.transition(
        command_id="command-1",
        owner=token,
        expected=CommandExecutionState.INTENT,
        new_state=CommandExecutionState.RUNNING,
        updated_at=NOW,
    )
    completion = InputEnvelope(
        "conversation", 2, "finished:command-1", 1, "command_finished", {"exit_code": 0}, NOW
    )

    assert await commands.finish_and_enqueue(
        command_id="command-1",
        owner=token,
        expected=CommandExecutionState.RUNNING,
        terminal_state=CommandExecutionState.FINISHED,
        result={"exit_code": 0},
        completion_input=completion,
        completed_at=NOW,
    )
    assert (await inbox.list_unfinished("conversation"))[-1].input_id == completion.input_id
    assert store.commands["command-1"]["state"] is CommandExecutionState.FINISHED


@pytest.mark.asyncio
async def test_shared_worker_directory_enforces_capacity_reservations():
    store = FakeSharedStore()
    directory = as_worker_directory(store)
    record = WorkerRecord("worker-1", "incarnation-1", 1, 0, 0, False, NOW)
    assert await directory.register(record)
    assert await directory.reserve(
        worker_id="worker-1", incarnation="incarnation-1", reservation_id="conversation-1"
    )
    assert not await directory.reserve(
        worker_id="worker-1", incarnation="incarnation-1", reservation_id="conversation-2"
    )
    assert await directory.release_reservation(
        worker_id="worker-1", incarnation="incarnation-1", reservation_id="conversation-1"
    )
    assert await directory.reserve(
        worker_id="worker-1", incarnation="incarnation-1", reservation_id="conversation-2"
    )


@pytest.mark.asyncio
async def test_shared_outbox_keeps_delivery_state_separate():
    store = FakeSharedStore()
    outbox = as_outbox_store(store)
    token = await as_ownership_store(store).acquire(
        conversation_key="conversation",
        worker_id="worker-1",
        worker_incarnation="incarnation-1",
        conversation_generation=1,
        now=NOW,
        lease_expires_at=NOW + timedelta(seconds=60),
    )
    assert token is not None
    assert await outbox.put_sanitized(
        output_id="output-1",
        conversation_key="conversation",
        conversation_generation=1,
        sequence=1,
        owner=token,
        payload={"text": "safe"},
        created_at=NOW,
    )
    claimed = await outbox.claim_next(
        conversation_key="conversation",
        sender_id="sender-1",
        now=NOW,
        lease_expires_at=NOW + timedelta(seconds=20),
    )
    assert claimed is not None
    assert claimed["state"].value == "pending"
    assert store.outputs["output-1"]["state"].value == "pending"


async def collect(events):
    return [event async for event in events]
