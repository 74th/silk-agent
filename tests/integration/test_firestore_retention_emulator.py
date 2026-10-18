"""`expire_at` is written when (and only when) a Firestore record is safe to delete."""

import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from google.cloud import firestore

from silk_agent.config import DistributedExecutionConfig
from silk_agent.core.distributed_models import (
    CommandExecutionState,
    InputDeliveryState,
    OutputDeliveryState,
    OwnershipToken,
    WorkerRecord,
)
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.codec import encode_record
from silk_agent.stores.firestore.command_monitor import FirestoreCommandMonitorStore
from silk_agent.stores.firestore.inbox import FirestoreInboxStore
from silk_agent.stores.firestore.outbox import FirestoreOutboxStore
from silk_agent.stores.firestore.quiesce import FirestoreQuiesceMailbox
from silk_agent.stores.firestore.retention import RetentionPolicy
from silk_agent.stores.firestore.workers import FirestoreWorkerDirectory

pytestmark = pytest.mark.integration

DAY = timedelta(days=1)
POLICY = RetentionPolicy(terminal=10 * DAY, receipt=3 * DAY, worker=2 * DAY)


@dataclass
class Env:
    client: firestore.AsyncClient
    prefix: str
    inbox: FirestoreInboxStore
    outbox: FirestoreOutboxStore
    monitor: FirestoreCommandMonitorStore

    def ref(self, collection: str, doc_id: str) -> firestore.AsyncDocumentReference:
        return self.client.collection(f"{self.prefix}_{collection}").document(doc_id)

    async def own(self, key: str, now: datetime, *, epoch: int = 1) -> OwnershipToken:
        owner = OwnershipToken(key, "worker-a", "incarnation-a", epoch, 1, now + timedelta(hours=1))
        await (
            self.inbox._collection("ownership")
            .document(self.inbox._conversation_id(key))
            .set({**encode_record(owner), "phase": "active"})
        )
        return owner

    async def accept(self, key: str, input_id: str, now: datetime, **kwargs) -> None:
        await self.inbox.accept_input(
            conversation_key=key,
            input_id=input_id,
            conversation_generation=1,
            kind=kwargs.pop("kind", "message"),
            payload=kwargs.pop("payload", {"text": "hello"}),
            created_at=now,
        )

    async def input_doc(self, key: str, input_id: str) -> dict:
        snapshot = await (
            self.inbox._collection("conversations")
            .document(self.inbox._conversation_id(key))
            .collection("inputs")
            .document(self._input_id(input_id))
            .get()
        )
        return snapshot.to_dict() or {}

    @staticmethod
    def _input_id(input_id: str) -> str:
        from silk_agent.stores.firestore.ids import document_id

        return document_id(input_id, prefix="input")

    async def advance(self, owner, input_id, expected, new_state, **kwargs) -> bool:
        return await self.inbox.advance_input(
            conversation_key=owner.conversation_key,
            input_id=input_id,
            owner=owner,
            expected=expected,
            new_state=new_state,
            **kwargs,
        )

    async def complete(self, owner, input_id) -> None:
        S = InputDeliveryState
        for expected, new_state, extra in (
            (S.PENDING, S.DISPATCHING, {"dispatch_attempt_id": "attempt-1"}),
            (S.DISPATCHING, S.QUEUED, {}),
            (S.QUEUED, S.ACCEPTED, {}),
            (S.ACCEPTED, S.COMPLETED, {}),
        ):
            assert await self.advance(owner, input_id, expected, new_state, **extra)


@pytest_asyncio.fixture
async def env() -> AsyncIterator[Env]:
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=f"silk-agent-test-{uuid.uuid4().hex[:12]}"),
        emulator_host=emulator_host,
    )
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    try:
        yield Env(
            client,
            prefix,
            FirestoreInboxStore(client, collection_prefix=prefix, retention=POLICY),
            FirestoreOutboxStore(client, collection_prefix=prefix, retention=POLICY),
            FirestoreCommandMonitorStore(client, collection_prefix=prefix, retention=POLICY),
        )
    finally:
        client.close()


def assert_expiry(data: dict, start: datetime, retention: timedelta, field: str = "expire_at"):
    value = data[field]
    assert isinstance(value, datetime), f"{field} must be a Timestamp, got {type(value)}"
    assert abs((value - (start + retention)).total_seconds()) < 60


@pytest.mark.asyncio
async def test_input_gets_timestamp_expiry_only_when_completed(env: Env):
    S = InputDeliveryState
    now = datetime.now(UTC)
    key = f"retention-input-{uuid.uuid4().hex}"
    await env.accept(key, "done", now)
    await env.accept(key, "unsure", now)
    owner = await env.own(key, now)

    assert "expire_at" not in await env.input_doc(key, "done")
    for expected, new_state, extra in (
        (S.PENDING, S.DISPATCHING, {"dispatch_attempt_id": "attempt-1"}),
        (S.DISPATCHING, S.QUEUED, {}),
        (S.QUEUED, S.ACCEPTED, {}),
    ):
        assert await env.advance(owner, "done", expected, new_state, **extra)
        assert "expire_at" not in await env.input_doc(key, "done")

    assert await env.advance(owner, "done", S.ACCEPTED, S.COMPLETED)
    done = await env.input_doc(key, "done")
    assert_expiry(done, now, 10 * DAY)
    assert isinstance(done["completed_at"], datetime)
    assert done["expire_at"] - done["completed_at"] == 10 * DAY

    # uncertain stays for operator recovery, however long it takes
    assert await env.advance(
        owner, "unsure", S.PENDING, S.DISPATCHING, dispatch_attempt_id="attempt-2"
    )
    assert await env.advance(owner, "unsure", S.DISPATCHING, S.UNCERTAIN)
    unsure = await env.input_doc(key, "unsure")
    assert unsure["delivery_state"] == "uncertain"
    assert "expire_at" not in unsure and "completed_at" not in unsure


@pytest.mark.asyncio
async def test_held_input_has_no_expiry_and_cancelled_one_has(env: Env):
    S = InputDeliveryState
    now = datetime.now(UTC)
    key = f"retention-held-{uuid.uuid4().hex}"
    await env.accept(key, "held", now)
    await env.accept(key, "stopped", now)
    owner = await env.own(key, now)
    assert await env.advance(owner, "held", S.PENDING, S.HELD)
    assert "expire_at" not in await env.input_doc(key, "held")
    assert await env.advance(owner, "stopped", S.PENDING, S.CANCELLED)
    assert_expiry(await env.input_doc(key, "stopped"), now, 10 * DAY)
    # held -> queued (the user resumed) still has none
    assert await env.advance(owner, "held", S.HELD, S.QUEUED)
    assert "expire_at" not in await env.input_doc(key, "held")


@pytest.mark.asyncio
async def test_rebind_after_reset_suppresses_stale_commands_with_expiry(env: Env):
    now = datetime.now(UTC)
    key = f"retention-rebind-{uuid.uuid4().hex}"
    await env.accept(key, "reset-seed", now)
    await env.accept(key, "late-notice", now, kind="command_finished", payload={})
    owner = await env.own(key, now, epoch=1)
    conversation_ref = env.inbox._collection("conversations").document(
        env.inbox._conversation_id(key)
    )
    await conversation_ref.set({"conversation_generation": 2}, merge=True)
    next_owner = OwnershipToken(key, "worker-a", "incarnation-a", 2, 2, now + timedelta(hours=1))
    await (
        env.inbox._collection("ownership")
        .document(env.inbox._conversation_id(key))
        .set({**encode_record(next_owner), "phase": "active"})
    )
    await env.inbox.rebind_after_reset(
        conversation_key=key, previous_generation=1, reset_seq=1, owner=next_owner
    )
    record = await env.input_doc(key, "late-notice")
    assert record["delivery_state"] == "suppressed"
    assert_expiry(record, now, 10 * DAY)
    assert owner.epoch == 1


@pytest.mark.asyncio
async def test_controls_and_queue_expire_when_handled_filtered_or_stale(env: Env):
    now = datetime.now(UTC)
    key = f"retention-control-{uuid.uuid4().hex}"
    for control_id in ("handled", "filtered", "stale"):
        await env.inbox.accept_control(
            conversation_key=key,
            input_id=control_id,
            conversation_generation=1,
            kind="stop_candidate",
            sender_id="U_ALLOWED",
            payload={"text": "STOP"},
            created_at=now,
        )
    owner = await env.own(key, now)

    async def docs(control_id):
        control = await env.inbox._control_ref(key, control_id).get()
        queue = await env.inbox._control_queue_ref(key, control_id).get()
        return control.to_dict() or {}, queue.to_dict() or {}

    assert await env.inbox.resolve_control_authorization(
        conversation_key=key, input_id="handled", authorized=True, now=now
    )
    control, queue = await docs("handled")
    assert control["authorization_state"] == "authorized"
    assert "expire_at" not in control and "expire_at" not in queue
    assert await env.inbox.mark_control_handled(
        conversation_key=key, input_id="handled", owner=owner, now=now
    )
    control, queue = await docs("handled")
    assert_expiry(control, now, 10 * DAY)
    assert_expiry(queue, now, 10 * DAY)

    assert await env.inbox.resolve_control_authorization(
        conversation_key=key, input_id="filtered", authorized=False, now=now
    )
    control, queue = await docs("filtered")
    assert_expiry(control, now, 10 * DAY)
    assert_expiry(queue, now, 10 * DAY)

    await (
        env.inbox._collection("conversations")
        .document(env.inbox._conversation_id(key))
        .set({"conversation_generation": 9}, merge=True)
    )
    assert await env.inbox.resolve_control_authorization(
        conversation_key=key, input_id="stale", authorized=True, now=now
    )
    control, queue = await docs("stale")
    assert control["authorization_state"] == "stale"
    assert_expiry(control, now, 10 * DAY)
    assert_expiry(queue, now, 10 * DAY)


@pytest.mark.asyncio
async def test_slack_receipts_record_reception_time_and_expiry(env: Env):
    now = datetime.now(UTC)
    key = f"retention-receipt-{uuid.uuid4().hex}"
    arguments = {
        "conversation_key": key,
        "input_id": "slack-input",
        "conversation_generation": 1,
        "kind": "message",
        "payload": {"text": "hi"},
        "created_at": now,
        "workspace": "T1",
        "team": "T1",
        "channel": "C1",
        "message_ts": "100.1",
        "event_id": "Ev1",
    }
    await env.inbox.accept_received(**arguments)
    receipts = [
        item async for item in env.client.collection(f"{env.prefix}_slack_receipts").stream()
    ]
    assert len(receipts) == 2
    for snapshot in receipts:
        data = snapshot.to_dict()
        assert isinstance(data["received_at"], datetime)
        assert_expiry(data, now, 3 * DAY)
    # A redelivery inside the retention is a duplicate: one input, receipts unchanged.
    await env.inbox.accept_received(**arguments)
    again = [item async for item in env.client.collection(f"{env.prefix}_slack_receipts").stream()]
    assert {s.id: s.to_dict()["expire_at"] for s in again} == {
        s.id: s.to_dict()["expire_at"] for s in receipts
    }
    assert (await env.input_doc(key, "slack-input"))["seq"] == 1


@pytest.mark.asyncio
async def test_outbox_expires_when_delivered_or_suppressed_not_when_uncertain(env: Env):
    now = datetime.now(UTC)
    key = f"retention-outbox-{uuid.uuid4().hex}"
    await env.accept(key, "seed", now)
    owner = await env.own(key, now)
    for sequence, output_id in enumerate(("delivered", "uncertain", "suppressed"), start=1):
        assert await env.outbox.put_sanitized(
            output_id=output_id,
            conversation_key=key,
            conversation_generation=1,
            sequence=sequence,
            owner=owner,
            payload={"kind": "message", "text": output_id},
            now=now,
            created_at=now,
        )

    async def output(output_id):
        from silk_agent.stores.firestore.ids import document_id

        return (await env.ref("outbox", document_id(output_id, prefix="output")).get()).to_dict()

    Out = OutputDeliveryState
    lease = now + timedelta(minutes=1)
    for output_id, new_state in (("delivered", Out.DELIVERED), ("uncertain", Out.UNCERTAIN)):
        claim = await env.outbox.claim_next(
            conversation_key=key, sender_id="sender", now=now, lease_expires_at=lease
        )
        assert claim is not None and claim["output_id"] == output_id
        assert "expire_at" not in await output(output_id)
        assert await env.outbox.mark_delivery(
            output_id=output_id,
            expected=Out.DISPATCHING,
            new_state=new_state,
            attempt_id=claim["attempt_id"],
            updated_at=now,
        )
    assert_expiry(await output("delivered"), now, 10 * DAY)
    assert "expire_at" not in await output("uncertain")

    # stale after a reset: revalidate_claim suppresses with an expiry
    await env.ref("conversations", env.inbox._conversation_id(key)).set(
        {"reset_pending": True}, merge=True
    )
    await env.ref("outbox", (await _doc_id("suppressed"))).update(
        {"status": "dispatching", "attempt_id": "attempt-s"}
    )
    assert not await env.outbox.revalidate_claim(
        output_id="suppressed", attempt_id="attempt-s", now=now
    )
    assert_expiry(await output("suppressed"), now, 10 * DAY)


async def _doc_id(output_id: str) -> str:
    from silk_agent.stores.firestore.ids import document_id

    return document_id(output_id, prefix="output")


@pytest.mark.asyncio
async def test_outbox_expiry_is_removed_if_a_record_goes_back(env: Env):
    now = datetime.now(UTC)
    ref = env.ref("outbox", await _doc_id("back"))
    await ref.set(
        {
            "output_id": "back",
            "status": "dispatching",
            "attempt_id": "a1",
            "expire_at": now + DAY,
        }
    )
    assert await env.outbox.mark_delivery(
        output_id="back",
        expected=OutputDeliveryState.DISPATCHING,
        new_state=OutputDeliveryState.PENDING,
        attempt_id="a1",
        next_attempt_at=now,
        updated_at=now,
    )
    assert "expire_at" not in (await ref.get()).to_dict()


async def _seed_command(env: Env, key: str, command_id: str, now: datetime, **extra) -> None:
    await env.monitor._ref(command_id).set(
        {
            "command_id": command_id,
            "conversation_key": key,
            "conversation_generation": 1,
            "sandbox_generation": 7,
            "state": CommandExecutionState.RUNNING.value,
            "delivery_mode": "background",
            "notification_state": "none",
            "monitor_lease_token": "lease",
            "next_attempt_at": now,
            **extra,
        }
    )
    from silk_agent.stores.firestore.ids import document_id

    await env.ref("sandbox_operations", document_id(command_id, prefix="operation")).set(
        {"operation_id": command_id, "state": "accepted"}
    )


async def _command_and_operation(env: Env, command_id: str) -> tuple[dict, dict]:
    from silk_agent.stores.firestore.ids import document_id

    command = (await env.monitor._ref(command_id).get()).to_dict() or {}
    operation = (
        await env.ref("sandbox_operations", document_id(command_id, prefix="operation")).get()
    ).to_dict() or {}
    return command, operation


@pytest.mark.asyncio
async def test_foreground_command_expires_with_its_operation_when_it_ends(env: Env):
    now = datetime.now(UTC)
    key = f"retention-command-{uuid.uuid4().hex}"
    command_id = f"command-{uuid.uuid4().hex}"
    await _seed_command(env, key, command_id, now, delivery_mode="foreground")
    result = await env.monitor.observe_terminal(
        command_id,
        claim_token="lease",
        expected=CommandExecutionState.RUNNING,
        terminal=CommandExecutionState.FINISHED,
        result={"exit_code": 0},
        now=now,
    )
    assert result is not None and result["decision"] == "return_result"
    command, operation = await _command_and_operation(env, command_id)
    assert_expiry(command, now, 10 * DAY)
    assert operation["expire_at"] == command["expire_at"]


@pytest.mark.asyncio
async def test_background_command_is_kept_until_its_notice_input_completes(env: Env):
    S = InputDeliveryState
    now = datetime.now(UTC)
    key = f"retention-notify-{uuid.uuid4().hex}"
    command_id = f"command-{uuid.uuid4().hex}"
    await env.accept(key, "seed", now)
    owner = await env.own(key, now)
    await _seed_command(env, key, command_id, now)
    result = await env.monitor.observe_terminal(
        command_id,
        claim_token="lease",
        expected=CommandExecutionState.RUNNING,
        terminal=CommandExecutionState.FINISHED,
        result={"exit_code": 0},
        now=now,
    )
    assert result is not None and result["decision"] == "notify"
    command, operation = await _command_and_operation(env, command_id)
    assert command["notification_state"] == "pending"
    assert "expire_at" not in command and "expire_at" not in operation
    notice_id = result["completion_input"].input_id

    # held (the turn was interrupted) and released again: still no expiry
    assert await env.advance(
        owner, notice_id, S.PENDING, S.DISPATCHING, dispatch_attempt_id="attempt-n"
    )
    assert await env.advance(owner, notice_id, S.DISPATCHING, S.QUEUED)
    assert await env.advance(owner, notice_id, S.QUEUED, S.HELD)
    assert await env.monitor.set_notification_state(
        command_id, expected="pending", new_state="held", now=now
    )
    assert await env.advance(owner, notice_id, S.HELD, S.QUEUED)
    assert await env.monitor.set_notification_state(
        command_id, expected="held", new_state="pending", now=now
    )
    command, operation = await _command_and_operation(env, command_id)
    assert "expire_at" not in command and "expire_at" not in operation

    assert await env.advance(owner, notice_id, S.QUEUED, S.ACCEPTED)
    command, operation = await _command_and_operation(env, command_id)
    assert command["notification_state"] == "pending" and "expire_at" not in command

    assert await env.advance(owner, notice_id, S.ACCEPTED, S.COMPLETED)
    command, operation = await _command_and_operation(env, command_id)
    assert command["notification_state"] == "delivered"
    assert_expiry(command, now, 10 * DAY)
    assert operation["expire_at"] == command["expire_at"]


@pytest.mark.asyncio
async def test_command_notice_that_never_reaches_the_agent_is_suppressed_and_expires(env: Env):
    S = InputDeliveryState
    now = datetime.now(UTC)
    key = f"retention-stopped-{uuid.uuid4().hex}"
    command_id = f"command-{uuid.uuid4().hex}"
    await env.accept(key, "seed", now)
    owner = await env.own(key, now)
    await _seed_command(env, key, command_id, now)
    result = await env.monitor.observe_terminal(
        command_id,
        claim_token="lease",
        expected=CommandExecutionState.RUNNING,
        terminal=CommandExecutionState.FINISHED,
        result={"exit_code": 0},
        now=now,
    )
    assert result is not None
    assert await env.advance(owner, result["completion_input"].input_id, S.PENDING, S.CANCELLED)
    command, operation = await _command_and_operation(env, command_id)
    assert command["notification_state"] == "suppressed"
    assert_expiry(command, now, 10 * DAY)
    assert operation["expire_at"] == command["expire_at"]


@pytest.mark.asyncio
async def test_cancelling_a_finished_command_with_a_pending_notice_expires_it(env: Env):
    now = datetime.now(UTC)
    key = f"retention-cancel-{uuid.uuid4().hex}"
    command_id = f"command-{uuid.uuid4().hex}"
    await _seed_command(
        env,
        key,
        command_id,
        now,
        state=CommandExecutionState.FINISHED.value,
        notification_state="pending",
        completed_at=now,
    )
    cancelled = await env.monitor.request_cancel(command_id, reason="user_stop", now=now)
    assert cancelled is not None and cancelled["cancel_outcome"] == "too_late"
    command, operation = await _command_and_operation(env, command_id)
    assert command["notification_state"] == "suppressed"
    assert_expiry(command, now, 10 * DAY)
    assert operation["expire_at"] == command["expire_at"]


@pytest.mark.asyncio
async def test_quiesce_request_expires_only_after_it_is_answered(env: Env):
    now = datetime.now(UTC)
    mailbox = FirestoreQuiesceMailbox(env.client, collection_prefix=env.prefix, retention=POLICY)
    token = OwnershipToken("quiesce-key", "worker-a", "incarnation-a", 1, 1, now + DAY)
    record = await mailbox.request(token, now=now)
    assert "expire_at" not in record
    assert "expire_at" not in (await mailbox.get(token) or {})
    await mailbox.answer(token, evidence={"stopped": True}, now=now)
    assert_expiry(await mailbox.get(token) or {}, now, 10 * DAY)


@pytest.mark.asyncio
async def test_worker_expiry_follows_the_heartbeat_and_admin_actions_keep_it(env: Env):
    directory = FirestoreWorkerDirectory(env.client, collection_prefix=env.prefix, retention=POLICY)
    first = datetime.now(UTC)
    record = WorkerRecord("worker-a", "incarnation-a", 4, 0, 0, False, first)
    assert await directory.register(record)
    ref = directory._worker_ref("worker-a")
    assert_expiry((await ref.get()).to_dict(), first, 2 * DAY)

    later = first + timedelta(hours=5)
    assert await directory.heartbeat(
        WorkerRecord("worker-a", "incarnation-a", 4, 0, 1, False, later)
    )
    heartbeated = (await ref.get()).to_dict()
    assert_expiry(heartbeated, later, 2 * DAY)

    updated = await directory.set_draining(
        worker_id="worker-a",
        incarnation="incarnation-a",
        draining=True,
        now=later + timedelta(hours=1),
    )
    assert updated is not None and updated.draining
    assert (await ref.get()).to_dict()["expire_at"] == heartbeated["expire_at"]


@pytest.mark.asyncio
async def test_history_reset_reason_is_saved_with_the_turn_state_and_read_back(env: Env):
    from silk_agent.core.ports import ConversationState
    from silk_agent.stores.firestore.conversations import FirestoreConversationStore
    from silk_agent.stores.firestore.session_state import FirestoreSessionStateStore

    now = datetime.now(UTC)
    key = f"retention-history-{uuid.uuid4().hex}"
    await env.accept(key, "seed", now)
    owner = await env.own(key, now)
    conversations = FirestoreConversationStore(env.client, collection_prefix=env.prefix)
    states = FirestoreSessionStateStore(conversations, lambda _key: owner)

    await states.put(ConversationState(key, agent_session_id="new-sdk-session", status="idle"))
    record = await conversations.get_conversation(key)
    assert record is not None and "history_reset_reason" not in record

    await states.put(
        ConversationState(
            key,
            agent_session_id="new-sdk-session",
            status="idle",
            history_reset_reason="expired",
        )
    )
    record = await conversations.get_conversation(key)
    assert record is not None
    assert record["sdk_session_id"] == "new-sdk-session"
    assert record["history_reset_reason"] == "expired"
    state = await states.get(key)
    assert state is not None and state.history_reset_reason == "expired"

    # A later turn that does not mention it keeps the record (it is only written when set).
    await states.put(ConversationState(key, agent_session_id="new-sdk-session", status="idle"))
    record = await conversations.get_conversation(key)
    assert record is not None and record["history_reset_reason"] == "expired"


async def _legacy_records(env: Env, now: datetime) -> dict[str, firestore.AsyncDocumentReference]:
    """Records as they were written before retention: terminal ones without `expire_at`."""
    from silk_agent.stores.firestore.ids import document_id

    key = f"legacy-{uuid.uuid4().hex}"
    refs: dict[str, firestore.AsyncDocumentReference] = {}
    conversation = env.ref("conversations", env.inbox._conversation_id(key))
    for name, state in (
        ("done", "completed"),
        ("cancelled", "cancelled"),
        ("unsure", "uncertain"),
        ("held", "held"),
        ("pending", "pending"),
    ):
        refs[f"input_{name}"] = conversation.collection("inputs").document(
            document_id(name, prefix="input")
        )
        await refs[f"input_{name}"].set(
            {"conversation_key": key, "input_id": name, "delivery_state": state, "seq": 1}
        )
    refs["control_done"] = conversation.collection("controls").document(
        document_id("c1", prefix="input")
    )
    await refs["control_done"].set({"authorization_state": "handled", "handled_at": now})
    refs["control_open"] = env.ref("control_queue", "open")
    await refs["control_open"].set({"authorization_state": "authorized"})
    refs["outbox_done"] = env.ref("outbox", "delivered")
    await refs["outbox_done"].set({"status": "delivered", "updated_at": now - 5 * DAY})
    refs["outbox_uncertain"] = env.ref("outbox", "uncertain")
    await refs["outbox_uncertain"].set({"status": "uncertain"})
    refs["command_done"] = env.ref("commands", document_id("cmd-done", prefix="command"))
    await refs["command_done"].set(
        {"state": "finished", "notification_state": "none", "completed_at": now - 2 * DAY}
    )
    refs["command_pending_notice"] = env.ref("commands", "cmd-notice")
    await refs["command_pending_notice"].set({"state": "finished", "notification_state": "pending"})
    refs["command_running"] = env.ref("commands", "cmd-running")
    await refs["command_running"].set({"state": "running", "notification_state": "none"})
    refs["operation_done"] = env.ref(
        "sandbox_operations", document_id("cmd-done", prefix="operation")
    )
    await refs["operation_done"].set(
        {"operation_id": "cmd-done", "operation_kind": "command_start"}
    )
    refs["operation_orphan"] = env.ref("sandbox_operations", "orphan")
    await refs["operation_orphan"].set({"operation_id": "orphan", "operation_kind": "file_write"})
    refs["quiesce_done"] = env.ref("quiesce_requests", "q1")
    await refs["quiesce_done"].set({"state": "answered"})
    refs["quiesce_open"] = env.ref("quiesce_requests", "q2")
    await refs["quiesce_open"].set({"state": "requested"})
    refs["receipt"] = env.ref("slack_receipts", "r1")
    await refs["receipt"].set({"input_id": "done"})
    refs["worker"] = env.ref("workers", "w1")
    await refs["worker"].set({"heartbeat_at": (now - 3 * DAY).isoformat()})
    return refs


@pytest.mark.asyncio
async def test_backfill_counts_by_default_writes_with_apply_and_is_idempotent(env: Env):
    from silk_agent.stores.firestore.retention_backfill import backfill

    now = datetime.now(UTC)
    refs = await _legacy_records(env, now)

    async def has_expiry(name: str) -> bool:
        return "expire_at" in ((await refs[name].get()).to_dict() or {})

    async def run(**kwargs):
        reports = await backfill(env.client, prefix=env.prefix, policy=POLICY, now=now, **kwargs)
        return {report.kind: report for report in reports}

    # default: counts only, nothing is written
    dry = await run()
    assert dry["inputs"].eligible == 2 and dry["inputs"].examined == 5
    assert dry["inputs"].excluded == {"state uncertain": 1, "state held": 1, "state pending": 1}
    assert dry["commands"].eligible == 1
    assert dry["sandbox_operations"].eligible == 1  # counted with its command, before it is written
    assert not any([await has_expiry(name) for name in refs])
    assert all(report.written == 0 for report in dry.values())

    # --limit bounds the eligible records handled per kind
    limited = await run(apply=True, limit=1)
    assert limited["inputs"].written == 1
    assert sum([await has_expiry(n) for n in ("input_done", "input_cancelled")]) == 1

    applied = await run(apply=True)
    assert applied["inputs"].written == 1  # the other terminal input

    done = (await refs["input_done"].get()).to_dict()
    assert isinstance(done["expire_at"], datetime)
    assert abs((done["expire_at"] - (now + 10 * DAY)).total_seconds()) < 60
    outbox = (await refs["outbox_done"].get()).to_dict()
    assert abs((outbox["expire_at"] - (now - 5 * DAY + 10 * DAY)).total_seconds()) < 60
    command = (await refs["command_done"].get()).to_dict()
    assert abs((command["expire_at"] - (now - 2 * DAY + 10 * DAY)).total_seconds()) < 60
    operation = (await refs["operation_done"].get()).to_dict()
    assert operation["expire_at"] == command["expire_at"]
    receipt = (await refs["receipt"].get()).to_dict()
    assert abs((receipt["expire_at"] - (now + 3 * DAY)).total_seconds()) < 60
    worker = (await refs["worker"].get()).to_dict()
    assert abs((worker["expire_at"] - (now - 3 * DAY + 2 * DAY)).total_seconds()) < 60

    # anything unfinished, uncertain, held or awaiting its notice is untouched
    for name in (
        "input_unsure",
        "input_held",
        "input_pending",
        "control_open",
        "outbox_uncertain",
        "command_pending_notice",
        "command_running",
        "operation_orphan",
        "quiesce_open",
    ):
        assert not await has_expiry(name), name

    # the second run finds nothing left to do
    again = await run(apply=True)
    assert all(report.eligible == 0 and report.written == 0 for report in again.values())
    assert again["inputs"].excluded["already has expire_at"] == 2


@pytest.mark.asyncio
async def test_backfill_only_touches_its_own_prefix(env: Env):
    from silk_agent.stores.firestore.ids import document_id
    from silk_agent.stores.firestore.retention_backfill import backfill

    now = datetime.now(UTC)
    other = f"other-{uuid.uuid4().hex[:8]}"
    key = f"prefix-{uuid.uuid4().hex}"
    cid = env.inbox._conversation_id(key)
    mine, theirs = [], []
    for prefix, refs in ((env.prefix, mine), (other, theirs)):
        conversation = env.client.collection(f"{prefix}_conversations").document(cid)
        for collection, field, value in (
            ("inputs", "delivery_state", "completed"),
            ("controls", "authorization_state", "handled"),
        ):
            ref = conversation.collection(collection).document(document_id("x", prefix="input"))
            await ref.set({field: value})
            refs.append(ref)

    for apply in (False, True):
        reports = {
            r.kind: r
            for r in await backfill(
                env.client, prefix=env.prefix, policy=POLICY, now=now, apply=apply, limit=1
            )
        }
        # --limit and the counts see only this prefix's records
        assert reports["inputs"].examined == 1 and reports["inputs"].eligible == 1
        assert reports["controls"].examined == 1 and reports["controls"].eligible == 1
    for ref in mine:
        assert "expire_at" in ((await ref.get()).to_dict() or {})
    for ref in theirs:
        assert "expire_at" not in ((await ref.get()).to_dict() or {})
