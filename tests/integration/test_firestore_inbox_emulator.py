import asyncio
import json
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.config import DistributedExecutionConfig
from silk_agent.core.clock import SystemClock
from silk_agent.core.distributed_models import (
    CommandExecutionState,
    InputDeliveryState,
    OutputDeliveryState,
    OwnershipToken,
    WorkerRecord,
)
from silk_agent.core.events import (
    ToolStarted,
    TurnFinished,
    TurnInterrupted,
    TurnStarted,
    UserMessage,
)
from silk_agent.core.ports import CommandResult
from silk_agent.core.session import AgentSessionRegistry, SessionSettings
from silk_agent.distributed.completion import CompletionInputDelivery
from silk_agent.distributed.dispatcher import ConversationDispatcher
from silk_agent.distributed.gateway import ReceivedSlackInput, SlackSocketGateway
from silk_agent.distributed.input_delivery import DurableInputDelivery
from silk_agent.distributed.reset import DistributedResetCoordinator
from silk_agent.distributed.stop import DistributedStopCoordinator
from silk_agent.distributed.worker import DistributedWorker
from silk_agent.sandbox.monitor import CommandMonitor
from silk_agent.stores.firestore.allocator import FirestoreAllocator
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.codec import encode_record
from silk_agent.stores.firestore.command_monitor import FirestoreCommandMonitorStore
from silk_agent.stores.firestore.conversations import FirestoreConversationStore
from silk_agent.stores.firestore.errors import FirestoreStoreError
from silk_agent.stores.firestore.ids import (
    conversation_document_id,
    document_id,
    worker_incarnation,
)
from silk_agent.stores.firestore.inbox import FirestoreInboxStore
from silk_agent.stores.firestore.outbox import FirestoreOutboxStore
from silk_agent.stores.firestore.ownership import FirestoreOwnershipStore
from silk_agent.stores.firestore.rate_limits import FirestoreUserRateLimitStore
from silk_agent.stores.firestore.service_operations import FirestoreServiceOperationStore
from silk_agent.stores.firestore.watch import FirestoreWatchAdapter
from silk_agent.stores.firestore.workers import FirestoreWorkerDirectory
from silk_agent.testing.fakes import FakeAgentEngine, FakeClock, Text, Tool
from tests.fakes.distributed_watch import FakeWatchSource

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_command_monitor_claim_is_exclusive_and_can_be_taken_over():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")

    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    first = FirestoreCommandMonitorStore(client, collection_prefix=prefix)
    second = FirestoreCommandMonitorStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    command_id = f"command-{uuid.uuid4().hex}"
    doc_id = document_id(command_id, prefix="command")
    ref = client.collection(f"{prefix}_commands").document(doc_id)
    bg_ref = None
    cancel_ref = None
    try:
        await ref.set(
            {
                "command_id": command_id,
                "conversation_key": "conversation-monitor-test",
                "conversation_generation": 1,
                "sandbox_generation": 1,
                "state": "running",
                "next_attempt_at": now,
            }
        )
        assert len(await first.list_due(now=now)) == 1
        left, right = await asyncio.gather(
            first.claim(
                command_id,
                monitor_id="monitor-a",
                now=now,
                lease_expires_at=now + timedelta(seconds=10),
            ),
            second.claim(
                command_id,
                monitor_id="monitor-b",
                now=now,
                lease_expires_at=now + timedelta(seconds=10),
            ),
            return_exceptions=True,
        )
        # A competing transaction can exhaust the emulator's short retry budget after
        # the winner commits. A fresh claim is safe because the operation is fenced by
        # the durable lease token and reads the current state before claiming again.
        if isinstance(left, Exception):
            if not isinstance(left, FirestoreStoreError) or left.status is not None:
                raise left
            left = await first.claim(
                command_id,
                monitor_id="monitor-a",
                now=now,
                lease_expires_at=now + timedelta(seconds=10),
            )
        if isinstance(right, Exception):
            if not isinstance(right, FirestoreStoreError) or right.status is not None:
                raise right
            right = await second.claim(
                command_id,
                monitor_id="monitor-b",
                now=now,
                lease_expires_at=now + timedelta(seconds=10),
            )
        assert (left is None) != (right is None)
        old = left or right
        assert old is not None
        claim_snapshot = await ref.get()
        assert await first.record_observation(
            command_id,
            claim_token=old["monitor_lease_token"],
            expected=CommandExecutionState.RUNNING,
            observed=CommandExecutionState.RUNNING,
            now=now,
            retry_at=now + timedelta(seconds=5),
        )
        assert (await ref.get()).update_time == claim_snapshot.update_time
        takeover_at = now + timedelta(seconds=11)
        replacement = await second.claim(
            command_id,
            monitor_id="monitor-b",
            now=takeover_at,
            lease_expires_at=takeover_at + timedelta(seconds=10),
        )
        assert replacement is not None
        assert (
            await first.observe_terminal(
                command_id,
                claim_token=old["monitor_lease_token"],
                expected=CommandExecutionState.RUNNING,
                terminal=CommandExecutionState.FINISHED,
                result={"exit_code": 0},
                now=takeover_at,
            )
            is None
        )
        finished = await second.observe_terminal(
            command_id,
            claim_token=replacement["monitor_lease_token"],
            expected=CommandExecutionState.RUNNING,
            terminal=CommandExecutionState.FINISHED,
            result={"exit_code": 0},
            now=takeover_at,
        )
        assert finished is not None and finished["decision"] == "return_result"
        assert (await ref.get()).to_dict()["state"] == "finished"

        bg_command_id = f"command-{uuid.uuid4().hex}"
        bg_ref = client.collection(f"{prefix}_commands").document(
            document_id(bg_command_id, prefix="command")
        )
        await bg_ref.set(
            {
                "command_id": bg_command_id,
                "conversation_key": "conversation-monitor-test",
                "conversation_generation": 1,
                "sandbox_generation": 1,
                "state": "running",
                "delivery_mode": "foreground",
                "notification_state": "none",
                "next_attempt_at": now,
            }
        )
        bg_claim = await first.claim(
            bg_command_id,
            monitor_id="monitor-a",
            now=now,
            lease_expires_at=now + timedelta(seconds=10),
        )
        assert bg_claim is not None
        detached = await first.detach(bg_command_id, now=now)
        assert detached is not None and detached["decision"] == "background"
        bg_finished = await first.observe_terminal(
            bg_command_id,
            claim_token=bg_claim["monitor_lease_token"],
            expected=CommandExecutionState.RUNNING,
            terminal=CommandExecutionState.FINISHED,
            result={
                "exit_code": 0,
                "output": {
                    "stdout": {
                        "uri": "gs://output-bucket/command-output/stdout",
                        "size_bytes": 64 * 1024 * 1024,
                        "sha256": "0" * 64,
                        "expires_at": (now + timedelta(days=30)).isoformat(),
                        "tail": "x" * 3072,
                    }
                },
            },
            now=now,
        )
        assert bg_finished is not None and bg_finished["decision"] == "notify"
        assert (await bg_ref.get()).to_dict()["notification_state"] == "pending"
        assert (await bg_ref.get()).to_dict()["result"]["output"]["stdout"]["size_bytes"] == (
            64 * 1024 * 1024
        )
        duplicate_finish = await first.observe_terminal(
            bg_command_id,
            claim_token=bg_claim["monitor_lease_token"],
            expected=CommandExecutionState.RUNNING,
            terminal=CommandExecutionState.FINISHED,
            result={"exit_code": 0},
            now=now,
        )
        assert duplicate_finish is None
        completion_id = f"command-finished:1:{bg_command_id}"
        conversation_ref = client.collection(f"{prefix}_conversations").document(
            conversation_document_id("conversation-monitor-test")
        )
        completion_ref = conversation_ref.collection("inputs").document(
            document_id(completion_id, prefix="input")
        )
        assert (await completion_ref.get()).to_dict()["input_id"] == completion_id
        assert (await conversation_ref.get()).to_dict()["next_input_seq"] == 2

        cancel_command_id = f"command-{uuid.uuid4().hex}"
        cancel_ref = client.collection(f"{prefix}_commands").document(
            document_id(cancel_command_id, prefix="command")
        )
        await cancel_ref.set(
            {
                "command_id": cancel_command_id,
                "conversation_key": "conversation-monitor-test",
                "conversation_generation": 1,
                "sandbox_generation": 1,
                "state": "running",
                "delivery_mode": "background",
                "notification_state": "none",
                "next_attempt_at": now,
            }
        )
        cancel_request = await first.request_cancel(cancel_command_id, reason="user_stop", now=now)
        assert cancel_request is not None
        assert cancel_request["state"] == "cancel_requested"
        assert cancel_request["notification_suppressed"] is True
        assert await first.record_cancel_result(
            cancel_command_id,
            request_id=cancel_request["cancel_request_id"],
            outcome="unknown",
            now=now,
        )
        cancel_claim = await first.claim(
            cancel_command_id,
            monitor_id="monitor-a",
            now=now,
            lease_expires_at=now + timedelta(seconds=10),
        )
        assert cancel_claim is not None
        cancel_race = await first.observe_terminal(
            cancel_command_id,
            claim_token=cancel_claim["monitor_lease_token"],
            expected=CommandExecutionState.CANCEL_REQUESTED,
            terminal=CommandExecutionState.FINISHED,
            result={"exit_code": 0},
            now=now,
        )
        assert cancel_race is not None and cancel_race["decision"] == "suppressed"
        final_cancel_record = (await cancel_ref.get()).to_dict()
        assert final_cancel_record["cancel_outcome"] == "not_confirmed"
        assert final_cancel_record["notification_state"] == "suppressed"
        assert (await conversation_ref.get()).to_dict()["next_input_seq"] == 2
    finally:
        await ref.delete()
        if bg_ref is not None:
            await bg_ref.delete()
        if cancel_ref is not None:
            await cancel_ref.delete()
        client.close()


@pytest.mark.asyncio
async def test_concurrent_duplicate_receipts_create_one_ordered_input():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")

    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    received_at = datetime.now(UTC)

    async def receive(
        conversation_key: str,
        input_id: str,
        *,
        message_ts: str = "123.456",
        event_id: str = "event-1",
        participating: bool = False,
    ):
        return await inbox.accept_received(
            conversation_key=conversation_key,
            input_id=input_id,
            conversation_generation=1,
            kind="message",
            payload={"text": "hello"},
            created_at=received_at,
            workspace="W1",
            team="T1",
            channel="C1",
            message_ts=message_ts,
            event_id=event_id,
            participating=participating,
        )

    try:
        first, second = await asyncio.gather(
            receive("conversation-a", "input-a", message_ts="1.1", event_id="event-a"),
            receive("conversation-a", "input-b", message_ts="1.2", event_id="event-b"),
        )
        assert sorted((first.seq, second.seq)) == [1, 2]
        pending = await inbox.list_unfinished(first.conversation_key)
        assert len(pending) == 2

        duplicate_event_first, duplicate_event_second = await asyncio.gather(
            receive("conversation-b", "input-c", message_ts="2.1", event_id="event-c"),
            receive("conversation-b", "input-d", message_ts="2.1", event_id="event-c"),
        )
        assert duplicate_event_first == duplicate_event_second
        assert len(await inbox.list_unfinished(duplicate_event_first.conversation_key)) == 1

        mention, message = await asyncio.gather(
            receive(
                "conversation-c",
                "input-e",
                message_ts="3.1",
                event_id="mention-event",
                participating=True,
            ),
            receive("conversation-c", "input-f", message_ts="3.1", event_id="message-event"),
        )
        assert mention == message
        assert len(await inbox.list_unfinished(mention.conversation_key)) == 1
        conversation = (
            await inbox._collection("conversations")
            .document(inbox._conversation_id("conversation-c"))
            .get()
        )
        assert conversation.to_dict()["participating"] is True
    finally:
        client.close()


@pytest.mark.asyncio
async def test_sender_enrichment_is_durable_and_retryable():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    try:
        await inbox.accept_received(
            conversation_key="enrichment-conversation",
            input_id="C1:100.1",
            conversation_generation=1,
            kind="message",
            payload={"text": "hello", "user": "U1"},
            created_at=now,
            workspace="T1",
            team="T1",
            channel="C1",
            message_ts="100.1",
            event_id="enrichment-event",
        )
        pending = await inbox.list_sender_enrichment_due(now=now)
        assert len(pending) == 1
        assert pending[0]["sender_lookup_user"] == "U1"
        retry_at = now + timedelta(seconds=10)
        assert await inbox.record_sender_enrichment(
            conversation_key="enrichment-conversation",
            input_id="C1:100.1",
            record_collection="inputs",
            sender_info=None,
            retry_at=retry_at,
            now=now,
        )
        assert await inbox.list_sender_enrichment_due(now=now) == ()
        assert await inbox.record_sender_enrichment(
            conversation_key="enrichment-conversation",
            input_id="C1:100.1",
            record_collection="inputs",
            sender_info={
                "email": "u@example.com",
                "is_guest": False,
                "is_external": False,
                "team": "T1",
            },
            retry_at=retry_at,
            now=retry_at,
        )
        assert await inbox.list_sender_enrichment_due(now=retry_at) == ()
        record = (
            await inbox._collection("conversations")
            .document(inbox._conversation_id("enrichment-conversation"))
            .collection("inputs")
            .document(document_id("C1:100.1", prefix="input"))
            .get()
        )
        assert record.to_dict()["payload"]["sender"]["email"] == "u@example.com"
        assert record.to_dict()["sender_lookup_state"] == "resolved"
    finally:
        client.close()


@pytest.mark.asyncio
async def test_stop_control_requires_authorization_before_advancing_stop_seq():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    try:
        for input_id, user_id, ts in (
            ("stop-denied", "U_DENIED", "200.1"),
            ("stop-allowed", "U_ALLOWED", "201.1"),
        ):
            await inbox.accept_received(
                conversation_key="stop-conversation",
                input_id=input_id,
                conversation_generation=1,
                kind="stop_candidate",
                payload={"text": "STOP"},
                created_at=now,
                workspace="T1",
                team="T1",
                channel="C1",
                message_ts=ts,
                event_id=f"event-{input_id}",
                sender_id=user_id,
            )
        # Simulate the Control process restarting after Gateway durably accepted
        # the Socket Mode events. The new store must recover both pending controls.
        inbox = FirestoreInboxStore(client, collection_prefix=prefix)
        controls = await inbox.list_pending_controls()
        assert len(controls) == 2
        assert await inbox.resolve_control_authorization(
            conversation_key="stop-conversation",
            input_id="stop-denied",
            authorized=False,
            now=now,
        )
        conversation_ref = inbox._collection("conversations").document(
            inbox._conversation_id("stop-conversation")
        )
        conversation = await conversation_ref.get()
        assert (conversation.to_dict() or {}).get("stop_seq", 0) == 0
        assert await inbox.resolve_control_authorization(
            conversation_key="stop-conversation",
            input_id="stop-allowed",
            authorized=True,
            now=now,
        )
        conversation = await conversation_ref.get()
        assert conversation.to_dict()["stop_seq"] == 2
    finally:
        client.close()


@pytest.mark.asyncio
async def test_authorized_stop_barrier_cancels_earlier_input_and_keeps_later_input():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    key = f"stop-barrier-{uuid.uuid4().hex}"
    owner = OwnershipToken(key, "worker-stop", "incarnation-stop", 1, 1, now + timedelta(minutes=5))
    owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))
    try:
        before = await inbox.accept_input(
            conversation_key=key,
            input_id="before-stop",
            conversation_generation=1,
            kind="message",
            payload={"text": "A"},
            created_at=now,
        )
        control = await inbox.accept_control(
            conversation_key=key,
            input_id="stop-control",
            conversation_generation=1,
            kind="stop_candidate",
            sender_id="U_ALLOWED",
            payload={"text": "STOP"},
            created_at=now,
        )
        after = await inbox.accept_input(
            conversation_key=key,
            input_id="after-stop",
            conversation_generation=1,
            kind="message",
            payload={"text": "B"},
            created_at=now,
        )
        assert (before.seq, control.seq, after.seq) == (1, 2, 3)
        await owner_ref.set({**encode_record(owner), "phase": "active"})
        assert await inbox.resolve_control_authorization(
            conversation_key=key,
            input_id=control.input_id,
            authorized=True,
            now=now,
        )
        assert not await inbox.advance_input(
            conversation_key=key,
            input_id=before.input_id,
            owner=owner,
            expected=InputDeliveryState.PENDING,
            new_state=InputDeliveryState.DISPATCHING,
            dispatch_attempt_id="late-guard-attempt",
        )

        async def cancel_commands():
            return "cancelled"

        summary = await DistributedStopCoordinator(
            inbox,
            conversations,
            owner,
            cancel_commands=cancel_commands,
            page_size=1,
        ).stop()
        assert summary.inputs_cancelled == 1
        assert summary.command_cancellation == "cancelled"
        records = {
            record.input_id: record
            for record in await inbox.list_unfinished(key, after_seq=0, limit=10)
        }
        assert before.input_id not in records
        assert records[after.input_id].delivery_state is InputDeliveryState.PENDING
        saved_conversation = await conversations.get_conversation(key)
        assert saved_conversation is not None
        assert saved_conversation["stop_seq"] == control.seq
    finally:
        client.close()


@pytest.mark.asyncio
async def test_stop_control_progresses_while_firestore_input_guard_is_waiting():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    key = f"stop-guard-{uuid.uuid4().hex}"
    owner = OwnershipToken(key, "worker-stop", "incarnation-stop", 1, 1, now + timedelta(minutes=5))
    owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))
    guard_entered = asyncio.Event()
    guard_release = asyncio.Event()
    stop_handled = asyncio.Event()
    after_dispatched = asyncio.Event()
    attempts = {}
    try:
        before = await inbox.accept_input(
            conversation_key=key,
            input_id="guarded-before-stop",
            conversation_generation=1,
            kind="message",
            payload={"text": "A"},
            created_at=now,
        )
        control = await inbox.accept_control(
            conversation_key=key,
            input_id="stop-during-guard",
            conversation_generation=1,
            kind="stop_candidate",
            sender_id="U_ALLOWED",
            payload={"text": "STOP"},
            created_at=now,
        )
        after = await inbox.accept_input(
            conversation_key=key,
            input_id="after-guard-stop",
            conversation_generation=1,
            kind="message",
            payload={"text": "B"},
            created_at=now,
        )
        await owner_ref.set({**encode_record(owner), "phase": "active"})

        async def read_inputs():
            return await inbox.list_unfinished(key, after_seq=0, limit=10)

        async def read_controls():
            if stop_handled.is_set():
                return ()
            return ({"input_id": control.input_id},)

        async def prepare_input(record):
            if record.input_id == before.input_id:
                guard_entered.set()
                await guard_release.wait()
            return record

        async def dispatch_input(record, _prepared):
            accepted = await inbox.advance_input(
                conversation_key=key,
                input_id=record.input_id,
                owner=owner,
                expected=InputDeliveryState.PENDING,
                new_state=InputDeliveryState.DISPATCHING,
                dispatch_attempt_id=f"attempt-{record.input_id}",
            )
            attempts[record.input_id] = accepted
            if record.input_id == after.input_id and accepted:
                after_dispatched.set()

        async def handle_control(_control):
            assert await inbox.resolve_control_authorization(
                conversation_key=key,
                input_id=control.input_id,
                authorized=True,
                now=datetime.now(UTC),
            )

            async def cancel_commands():
                return None

            await DistributedStopCoordinator(
                inbox, conversations, owner, cancel_commands=cancel_commands
            ).stop()
            stop_handled.set()

        dispatcher = ConversationDispatcher(
            read_inputs=read_inputs,
            read_controls=read_controls,
            prepare_input=prepare_input,
            dispatch_input=dispatch_input,
            handle_control=handle_control,
            run_turns=lambda stop: stop.wait(),
            poll_seconds=0.01,
        )
        running = asyncio.create_task(dispatcher.run())
        await asyncio.wait_for(guard_entered.wait(), timeout=2)
        await asyncio.wait_for(stop_handled.wait(), timeout=2)
        assert attempts == {}
        guard_release.set()
        await asyncio.wait_for(after_dispatched.wait(), timeout=2)
        dispatcher.stop()
        await running
        # A was cancelled by the stop barrier: it never dispatches successfully. Depending on
        # whether the reader dropped its task before the guard returned, it either never tries
        # or tries late and is fenced (False). B proceeds without waiting for A's guard.
        assert attempts.get(before.input_id) in (None, False)
        assert attempts[after.input_id] is True
        saved = await conversations.get_conversation(key)
        assert saved is not None and saved["stop_seq"] == control.seq
    finally:
        guard_release.set()
        client.close()


@pytest.mark.asyncio
async def test_firestore_input_returns_to_pending_after_session_append_rejection():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    key = f"append-rejection-{uuid.uuid4().hex}"
    owner = OwnershipToken(
        key, "worker-append", "incarnation-append", 1, 1, now + timedelta(minutes=5)
    )
    owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))

    class Session:
        def __init__(self):
            self.outcomes = ["rejected", "appended"]
            self.messages = []

        async def submit(self, message, *, before_queue=None):
            self.messages.append(message)
            return self.outcomes.pop(0)

    try:
        envelope = await inbox.accept_input(
            conversation_key=key,
            input_id="append-refused-input",
            conversation_generation=1,
            kind="message",
            payload={"text": "retry without loss"},
            created_at=now,
        )
        await owner_ref.set({**encode_record(owner), "phase": "active"})
        session = Session()
        delivery = DurableInputDelivery(inbox, owner, session)
        message = UserMessage(key, "U1", "retry without loss")
        assert await delivery.dispatch(envelope, message) == "rejected"
        pending = await inbox.list_unfinished(key, after_seq=0, limit=10)
        assert len(pending) == 1
        assert pending[0].input_id == envelope.input_id
        assert pending[0].delivery_state is InputDeliveryState.PENDING

        assert await delivery.dispatch(pending[0], message) == "appended"
        accepted = await inbox.list_unfinished(key, after_seq=0, limit=10)
        assert len(accepted) == 1
        assert accepted[0].delivery_state is InputDeliveryState.ACCEPTED
        assert len(session.messages) == 2
        assert session.messages[0].dispatch_attempt_id
        assert session.messages[1].dispatch_attempt_id
        assert session.messages[0].dispatch_attempt_id != session.messages[1].dispatch_attempt_id
    finally:
        client.close()


@pytest.mark.parametrize("mode", ["append", "interrupt", "queue"])
@pytest.mark.asyncio
async def test_firestore_durable_delivery_composes_with_agent_session_modes(mode, channel):
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    clock = FakeClock(datetime.now(UTC))
    key = f"session-mode-{mode}-{uuid.uuid4().hex}"
    owner = OwnershipToken(
        key, "worker-session", "incarnation-session", 1, 1, clock.now() + timedelta(minutes=5)
    )
    owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))
    holder = {}
    engines = []
    completed_inputs = set()
    completion_event = asyncio.Event()

    async def cancel_commands(_key):
        raise AssertionError("new-message interruption must preserve background commands")

    async def complete_input(input_id):
        if await holder["delivery"].complete(input_id):
            completed_inputs.add(input_id)
            completion_event.set()

    registry = AgentSessionRegistry(
        lambda _key: (
            engines.append(
                FakeAgentEngine(
                    clock, lambda request: [Tool("bash", duration=5), Text(request.text)]
                )
            )
            or engines[-1]
        ),
        channel.send,
        clock,
        SessionSettings(interrupt_mode=mode),
        cancel_commands=cancel_commands,
        input_started=lambda input_id: holder["delivery"].mark_started(input_id),
        input_completed=complete_input,
        input_uncertain=lambda input_id: holder["delivery"].mark_uncertain(input_id),
    )
    try:
        first = await inbox.accept_input(
            conversation_key=key,
            input_id="first",
            conversation_generation=1,
            kind="message",
            payload={"text": "A"},
            created_at=clock.now(),
        )
        second = await inbox.accept_input(
            conversation_key=key,
            input_id="second",
            conversation_generation=1,
            kind="message",
            payload={"text": "B"},
            created_at=clock.now(),
        )
        await owner_ref.set({**encode_record(owner), "phase": "active"})
        session = await registry.get_or_create(key)
        delivery = DurableInputDelivery(inbox, owner, session)
        holder["delivery"] = delivery

        assert await delivery.dispatch(first, UserMessage(key, "U1", "A")) == "queued"
        await channel.wait_for(ToolStarted)
        outcome = await delivery.dispatch(second, UserMessage(key, "U1", "B"))
        assert outcome == ("appended" if mode == "append" else "queued")

        if mode == "interrupt":
            await channel.wait_for(TurnInterrupted)
            await channel.wait_for(ToolStarted, count=2)
            await clock.advance(5)
            await channel.wait_for(TurnFinished)
        else:
            await clock.advance(5)
            await channel.wait_for(TurnFinished)
            if mode == "queue":
                await channel.wait_for(ToolStarted, count=2)
                await clock.advance(5)
                await channel.wait_for(TurnFinished, count=2)

        expected_completed = {first.input_id, second.input_id} - (
            {first.input_id} if mode == "interrupt" else set()
        )
        while completed_inputs != expected_completed:
            completion_event.clear()
            await asyncio.wait_for(completion_event.wait(), timeout=2)
        assert len(engines) == 1
        unfinished = await inbox.list_unfinished(key, after_seq=0, limit=10)
        if mode == "interrupt":
            assert [(item.input_id, item.delivery_state) for item in unfinished] == [
                (first.input_id, InputDeliveryState.ACCEPTED)
            ]
        else:
            assert unfinished == ()
    finally:
        await registry.aclose()
        client.close()


@pytest.mark.asyncio
async def test_firestore_input_queues_when_append_closes_at_turn_end(channel):
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    clock = FakeClock(now)
    key = f"append-at-end-{uuid.uuid4().hex}"
    owner = OwnershipToken(
        key, "worker-append", "incarnation-append", 1, 1, now + timedelta(minutes=5)
    )
    owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))
    holder = {}
    engine_holder = []
    completed_inputs = set()
    completion_event = asyncio.Event()

    async def complete_input(input_id):
        if await holder["delivery"].complete(input_id):
            completed_inputs.add(input_id)
            completion_event.set()

    registry = AgentSessionRegistry(
        lambda _key: (
            engine_holder.append(
                FakeAgentEngine(
                    clock, lambda request: [Tool("bash", duration=5), Text(request.text)]
                )
            )
            or engine_holder[-1]
        ),
        channel.send,
        clock,
        SessionSettings(interrupt_mode="append"),
        input_started=lambda input_id: holder["delivery"].mark_started(input_id),
        input_completed=complete_input,
        input_uncertain=lambda input_id: holder["delivery"].mark_uncertain(input_id),
    )
    try:
        first = await inbox.accept_input(
            conversation_key=key,
            input_id="first",
            conversation_generation=1,
            kind="message",
            payload={"text": "A"},
            created_at=now,
        )
        second = await inbox.accept_input(
            conversation_key=key,
            input_id="second",
            conversation_generation=1,
            kind="message",
            payload={"text": "B"},
            created_at=now,
        )
        await owner_ref.set({**encode_record(owner), "phase": "active"})
        session = await registry.get_or_create(key)
        delivery = DurableInputDelivery(inbox, owner, session)
        holder["delivery"] = delivery

        assert await delivery.dispatch(first, UserMessage(key, "U1", "A")) == "queued"
        await channel.wait_for(ToolStarted)

        async def refuse_append(_text):
            return False

        engine_holder[0].append = refuse_append
        assert await delivery.dispatch(second, UserMessage(key, "U1", "B")) == "queued"
        queued = await inbox.list_unfinished(key, after_seq=0, limit=10)
        assert queued[1].delivery_state is InputDeliveryState.QUEUED

        await clock.advance(5)
        await channel.wait_for(TurnFinished)
        await channel.wait_for(ToolStarted, count=2)
        await clock.advance(5)
        await channel.wait_for(TurnFinished, count=2)
        while completed_inputs != {first.input_id, second.input_id}:
            completion_event.clear()
            await asyncio.wait_for(completion_event.wait(), timeout=2)
        assert await inbox.list_unfinished(key, after_seq=0, limit=10) == ()
        assert len(engine_holder) == 1
    finally:
        await registry.aclose()
        client.close()


@pytest.mark.asyncio
async def test_authorized_stop_interrupts_tool_wait_without_waiting_for_turn_completion(channel):
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    clock = FakeClock(now)
    key = f"stop-tool-wait-{uuid.uuid4().hex}"
    owner = OwnershipToken(key, "worker-stop", "incarnation-stop", 1, 1, now + timedelta(minutes=5))
    owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))
    holder = {}
    engines = []
    session_cancel_calls = []

    async def session_cancel_commands(_key):
        session_cancel_calls.append(_key)

    registry = AgentSessionRegistry(
        lambda _key: (
            engines.append(FakeAgentEngine(clock, lambda _request: [Tool("bash", duration=3600)]))
            or engines[-1]
        ),
        channel.send,
        clock,
        SessionSettings(interrupt_mode="append"),
        cancel_commands=session_cancel_commands,
        input_started=lambda input_id: holder["delivery"].mark_started(input_id),
        input_completed=lambda input_id: holder["delivery"].complete(input_id),
        input_uncertain=lambda input_id: holder["delivery"].mark_uncertain(input_id),
    )
    try:
        envelope = await inbox.accept_input(
            conversation_key=key,
            input_id="long-tool-input",
            conversation_generation=1,
            kind="message",
            payload={"text": "run a long tool"},
            created_at=now,
        )
        control = await inbox.accept_control(
            conversation_key=key,
            input_id="stop-long-tool",
            conversation_generation=1,
            kind="stop_candidate",
            sender_id="U_ALLOWED",
            payload={"text": "STOP"},
            created_at=now,
        )
        await owner_ref.set({**encode_record(owner), "phase": "active"})
        session = await registry.get_or_create(key)
        delivery = DurableInputDelivery(inbox, owner, session)
        holder["delivery"] = delivery
        assert (
            await delivery.dispatch(envelope, UserMessage(key, "U1", "run a long tool")) == "queued"
        )
        await channel.wait_for(ToolStarted)
        assert await inbox.resolve_control_authorization(
            conversation_key=key,
            input_id=control.input_id,
            authorized=True,
            now=datetime.now(UTC),
        )
        coordinated_cancellations = []

        async def cancel_tracked_commands():
            coordinated_cancellations.append("requested")

        async def interrupt_turn():
            await session.request_stop(reason="user")

        summary = await DistributedStopCoordinator(
            inbox,
            conversations,
            owner,
            interrupt_turn=interrupt_turn,
            cancel_commands=cancel_tracked_commands,
        ).stop()
        assert summary.interrupt_requested
        assert coordinated_cancellations == ["requested"]
        assert session_cancel_calls == [key]
        await channel.wait_for(TurnInterrupted)
        assert len(engines) == 1 and engines[0].interrupt_count == 1
        assert channel.of(TurnFinished) == []
        saved = await conversations.get_conversation(key)
        assert saved is not None and saved["stop_seq"] == control.seq
    finally:
        await registry.aclose()
        client.close()


@pytest.mark.asyncio
async def test_reset_interrupts_running_tool_and_rebinds_later_input(channel):
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    clock = FakeClock(now)
    key = f"reset-tool-wait-{uuid.uuid4().hex}"
    owner = OwnershipToken(
        key, "worker-reset", "incarnation-reset", 1, 1, now + timedelta(minutes=5)
    )
    owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))
    holder = {}
    engines = []

    async def save_event(event):
        await channel.send(event)
        state = (
            "running"
            if isinstance(event, TurnStarted)
            else "interrupted_user"
            if isinstance(event, TurnInterrupted)
            else "idle"
            if isinstance(event, TurnFinished)
            else None
        )
        if state is None:
            return
        current = await conversations.get_conversation(key)
        assert current is not None
        patch = {"turn_status": state}
        if isinstance(event, TurnStarted):
            patch["turn_id"] = event.turn_id
        assert (
            await conversations.commit_turn_state(
                owner=owner,
                expected_version=int(current.get("version", 1)),
                patch=patch,
            )
            is not None
        )

    async def cancel_commands(_key):
        return None

    async def cancel_tracked_commands():
        return None

    registry = AgentSessionRegistry(
        lambda _key: (
            engines.append(FakeAgentEngine(clock, lambda _request: [Tool("bash", duration=3600)]))
            or engines[-1]
        ),
        save_event,
        clock,
        SessionSettings(interrupt_mode="append"),
        cancel_commands=cancel_commands,
        input_started=lambda input_id: holder["delivery"].mark_started(input_id),
        input_completed=lambda input_id: holder["delivery"].complete(input_id),
        input_uncertain=lambda input_id: holder["delivery"].mark_uncertain(input_id),
    )
    try:
        before = await inbox.accept_input(
            conversation_key=key,
            input_id="running-before-reset",
            conversation_generation=1,
            kind="message",
            payload={"text": "long turn"},
            created_at=now,
        )
        reset_control = await inbox.accept_control(
            conversation_key=key,
            input_id="reset-running-turn",
            conversation_generation=1,
            kind="reset_candidate",
            sender_id="U_ALLOWED",
            payload={"text": "reset"},
            created_at=now,
        )
        later = await inbox.accept_input(
            conversation_key=key,
            input_id="after-running-reset",
            conversation_generation=1,
            kind="message",
            payload={"text": "keep this request"},
            created_at=now,
        )
        await owner_ref.set({**encode_record(owner), "phase": "active"})
        session = await registry.get_or_create(key)
        delivery = DurableInputDelivery(inbox, owner, session)
        holder["delivery"] = delivery
        assert await delivery.dispatch(before, UserMessage(key, "U1", "long turn")) == "queued"
        await channel.wait_for(ToolStarted)
        assert await inbox.resolve_control_authorization(
            conversation_key=key,
            input_id=reset_control.input_id,
            authorized=True,
            now=datetime.now(UTC),
        )
        updated_owners = []

        async def interrupt_and_wait():
            await session.stop(reason="reset")

        async def update_actor(token):
            updated_owners.append(token)

        result = await DistributedResetCoordinator(
            inbox,
            conversations,
            clock,
            owner,
            stop=DistributedStopCoordinator(
                inbox,
                conversations,
                owner,
                interrupt_turn=interrupt_and_wait,
                cancel_commands=cancel_tracked_commands,
            ),
            update_actor=update_actor,
            reset_seq=reset_control.seq,
        ).reset()
        assert result is not None
        assert result.owner.conversation_generation == 2
        assert updated_owners == [result.owner]
        assert len(engines) == 1 and engines[0].interrupt_count == 1
        assert channel.of(TurnFinished) == []
        rebound = await inbox.list_unfinished(key, after_seq=reset_control.seq, limit=10)
        assert [(item.input_id, item.conversation_generation) for item in rebound] == [
            (later.input_id, 2)
        ]
        saved = await conversations.get_conversation(key)
        assert saved is not None and saved["turn_status"] == "idle"
    finally:
        await registry.aclose()
        client.close()


@pytest.mark.asyncio
async def test_stop_after_completion_creation_suppresses_and_cancels_notice():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    monitor = FirestoreCommandMonitorStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    key = f"completion-stop-{uuid.uuid4().hex}"
    command_id = f"command-{uuid.uuid4().hex}"
    owner = OwnershipToken(
        key, "worker-completion", "incarnation-completion", 1, 1, now + timedelta(minutes=5)
    )
    owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))

    class Session:
        def __init__(self):
            self.notifications = []

        async def notify(self, text, *, input_id=""):
            self.notifications.append((input_id, text))
            return "queued"

    try:
        await inbox.accept_input(
            conversation_key=key,
            input_id="seed-input",
            conversation_generation=1,
            kind="message",
            payload={"text": "seed"},
            created_at=now,
        )
        await owner_ref.set({**encode_record(owner), "phase": "active"})
        command_ref = monitor._ref(command_id)
        await command_ref.set(
            {
                "command_id": command_id,
                "conversation_key": key,
                "conversation_generation": 1,
                "sandbox_generation": 7,
                "state": CommandExecutionState.RUNNING.value,
                "delivery_mode": "background",
                "notification_state": "none",
                "monitor_lease_token": "monitor-lease-token",
                "next_attempt_at": now,
            }
        )
        terminal = await monitor.observe_terminal(
            command_id,
            claim_token="monitor-lease-token",
            expected=CommandExecutionState.RUNNING,
            terminal=CommandExecutionState.FINISHED,
            result={"exit_code": 0},
            now=now,
        )
        assert terminal is not None and terminal["decision"] == "notify"
        completion = terminal["completion_input"]
        assert completion is not None
        control = await inbox.accept_control(
            conversation_key=key,
            input_id="stop-after-completion",
            conversation_generation=1,
            kind="stop_candidate",
            sender_id="U_ALLOWED",
            payload={"text": "STOP"},
            created_at=now,
        )
        assert await inbox.resolve_control_authorization(
            conversation_key=key,
            input_id=control.input_id,
            authorized=True,
            now=now,
        )

        async def cancel_commands():
            cancelled = await monitor.request_cancel(command_id, reason="user_stop", now=now)
            assert cancelled is not None and cancelled["cancel_outcome"] == "too_late"

        stop = DistributedStopCoordinator(
            inbox, conversations, owner, cancel_commands=cancel_commands
        )
        summary = await stop.stop()
        assert summary.inputs_cancelled == 2
        session = Session()
        outcome = await CompletionInputDelivery(inbox, owner, session).dispatch(completion)
        assert outcome == "claimed_elsewhere"
        assert session.notifications == []
        command = await monitor.get_command(command_id)
        assert command is not None and command["notification_state"] == "suppressed"
    finally:
        client.close()


@pytest.mark.asyncio
async def test_shared_user_rate_limit_is_atomic_and_idempotent():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=f"silk-agent-test-{uuid.uuid4().hex[:12]}"),
        emulator_host=emulator_host,
    )
    store = FirestoreUserRateLimitStore(client, collection_prefix=f"test-{uuid.uuid4().hex[:12]}")
    now = datetime.now(UTC)
    try:
        assert await store.allow_and_record(
            user_key="T1:U1", input_id="message-1", now=now, per_minute=2
        )
        assert await store.allow_and_record(
            user_key="T1:U1", input_id="message-1", now=now, per_minute=2
        )
        assert await store.allow_and_record(
            user_key="T1:U1", input_id="message-2", now=now, per_minute=2
        )
        assert not await store.allow_and_record(
            user_key="T1:U1", input_id="message-3", now=now, per_minute=2
        )
        later = now + timedelta(seconds=61)
        assert await store.allow_and_record(
            user_key="T1:U1", input_id="message-3", now=later, per_minute=2
        )
    finally:
        client.close()


@pytest.mark.asyncio
async def test_guard_history_is_fenced_and_keeps_only_the_last_five():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    store = FirestoreConversationStore(client, collection_prefix=prefix)
    key = f"history-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    owner = OwnershipToken(key, "worker-a", "incarnation-a", 1, 1, now + timedelta(minutes=1))
    conversation_ref = store._collection("conversations").document(store._conversation_id(key))
    ownership_ref = store._collection("ownership").document(store._conversation_id(key))
    try:
        await conversation_ref.set(
            {"conversation_key": key, "conversation_generation": 1, "version": 1}
        )
        await ownership_ref.set({**owner.to_dict(), "phase": "active"})
        assert await store.load_guard_history(key) == ()
        for index in range(7):
            assert await store.append_guard_history(owner=owner, text=f"message-{index}")
        assert await store.load_guard_history(key) == tuple(f"message-{i}" for i in range(2, 7))
        stale = OwnershipToken(key, "worker-a", "incarnation-old", 1, 1, now + timedelta(minutes=1))
        assert not await store.append_guard_history(owner=stale, text="stale")
    finally:
        client.close()


@pytest.mark.asyncio
async def test_query_session_id_is_saved_before_sdk_dispatch():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    key = f"query-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    owner = OwnershipToken(key, "worker-a", "incarnation-a", 1, 1, now + timedelta(minutes=1))
    try:
        envelope = await inbox.accept_input(
            conversation_key=key,
            input_id="input-query",
            conversation_generation=1,
            kind="message",
            payload={"text": "hello"},
            created_at=now,
        )
        conversation_ref = inbox._collection("conversations").document(inbox._conversation_id(key))
        owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))
        await owner_ref.set({**owner.to_dict(), "phase": "active"})
        await conversation_ref.update({"stop_seq": envelope.seq})
        assert not await inbox.advance_input(
            conversation_key=key,
            input_id=envelope.input_id,
            owner=owner,
            expected=InputDeliveryState.PENDING,
            new_state=InputDeliveryState.DISPATCHING,
            dispatch_attempt_id="blocked-by-stop",
        )
        await conversation_ref.update({"stop_seq": 0})
        assert await inbox.advance_input(
            conversation_key=key,
            input_id=envelope.input_id,
            owner=owner,
            expected=InputDeliveryState.PENDING,
            new_state=InputDeliveryState.DISPATCHING,
            dispatch_attempt_id="query-attempt",
        )
        assert await inbox.advance_input(
            conversation_key=key,
            input_id=envelope.input_id,
            owner=owner,
            expected=InputDeliveryState.DISPATCHING,
            new_state=InputDeliveryState.ACCEPTED,
        )
        assert await inbox.record_query_start(
            conversation_key=key,
            input_id=envelope.input_id,
            owner=owner,
            dispatch_attempt_id="query-attempt",
            sdk_session_id="sdk-session-1",
            now=now,
        )
        record = (
            await conversation_ref.collection("inputs")
            .document(document_id(envelope.input_id, prefix="input"))
            .get()
        )
        assert record.to_dict()["sdk_session_id"] == "sdk-session-1"
        assert (await conversation_ref.get()).get("sdk_session_id") == "sdk-session-1"
    finally:
        client.close()


@pytest.mark.asyncio
async def test_command_monitor_pages_stop_targets_by_conversation_generation_and_seq():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    monitor = FirestoreCommandMonitorStore(client, collection_prefix=prefix)
    try:
        for command_id, state, generation, seq in (
            ("stop-target-1", "running", 2, 4),
            ("stop-target-2", "cancel_requested", 2, 5),
            ("after-barrier", "running", 2, 6),
            ("old-generation", "running", 1, 4),
            ("terminal", "finished", 2, 4),
        ):
            await monitor._ref(command_id).set(
                {
                    "command_id": command_id,
                    "conversation_key": "stop-target-conversation",
                    "conversation_generation": generation,
                    "origin_seq": seq,
                    "state": state,
                    "schema_version": 1,
                }
            )
        first, cursor = await monitor.list_for_stop(
            conversation_key="stop-target-conversation",
            conversation_generation=2,
            stop_seq=5,
            limit=1,
        )
        assert [record["command_id"] for record in first] == ["stop-target-1"]
        assert cursor is not None
        second, next_cursor = await monitor.list_for_stop(
            conversation_key="stop-target-conversation",
            conversation_generation=2,
            stop_seq=5,
            limit=1,
            cursor=cursor,
        )
        assert [record["command_id"] for record in second] == ["stop-target-2"]
        assert next_cursor is None
    finally:
        client.close()


@pytest.mark.asyncio
async def test_control_only_ownership_activates_without_consuming_sdk_capacity():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    ownership = FirestoreOwnershipStore(client, collection_prefix=prefix)
    workers = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    key = f"control-only-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    worker = WorkerRecord("worker-a", "incarnation-a", 2, 1, 0, False, now)
    owner = OwnershipToken(
        key, worker.worker_id, worker.incarnation, 1, 1, now + timedelta(minutes=1)
    )
    owner_ref, conversation_ref = ownership._refs(key)
    worker_ref, reservation_ref = ownership._worker_refs(worker.worker_id, worker.incarnation, key)
    try:
        await workers.register(worker)
        await conversation_ref.set(
            {"conversation_generation": 1, "allocation_state": "assigned", "control_only": True}
        )
        await owner_ref.set({**owner.to_dict(), "phase": "assigned"})
        await reservation_ref.set({"conversation_key": key, "schema_version": 1})
        assert await ownership.activate_control(owner, now=now)
        worker_data = (await worker_ref.get()).to_dict()
        owner_data = (await owner_ref.get()).to_dict()
        assert worker_data["reserved"] == 1 and worker_data["active_sdk"] == 0
        assert owner_data["phase"] == "active" and owner_data["sdk_active"] is False
        assert await ownership.release(owner, now=now)
        assert (await worker_ref.get()).get("active_sdk") == 0
    finally:
        client.close()


@pytest.mark.asyncio
async def test_owner_epoch_fences_dispatch_and_recovery_marks_uncertain():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")

    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversation_key = f"conversation-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    expires_at = now.replace(year=now.year + 1)
    owner = OwnershipToken(conversation_key, "worker-a", "incarnation-a", 1, 1, expires_at)
    try:
        envelope = await inbox.accept_received(
            conversation_key=conversation_key,
            input_id="input-1",
            conversation_generation=1,
            kind="message",
            payload={"text": "hello"},
            created_at=now,
            workspace="W1",
            team="T1",
            channel="C1",
            message_ts="4.1",
            event_id="event-4",
        )
        conversation_id = inbox._conversation_id(conversation_key)
        ownership_ref = inbox._collection("ownership").document(conversation_id)
        await ownership_ref.set({**encode_record(owner), "phase": "active"})

        assert await inbox.advance_input(
            conversation_key=conversation_key,
            input_id=envelope.input_id,
            owner=owner,
            expected=InputDeliveryState.PENDING,
            new_state=InputDeliveryState.DISPATCHING,
            dispatch_attempt_id="attempt-1",
        )
        assert await inbox.advance_input(
            conversation_key=conversation_key,
            input_id=envelope.input_id,
            owner=owner,
            expected=InputDeliveryState.DISPATCHING,
            new_state=InputDeliveryState.PENDING,
        )
        assert await inbox.advance_input(
            conversation_key=conversation_key,
            input_id=envelope.input_id,
            owner=owner,
            expected=InputDeliveryState.PENDING,
            new_state=InputDeliveryState.DISPATCHING,
            dispatch_attempt_id="attempt-2",
        )

        replacement = OwnershipToken(
            conversation_key,
            "worker-b",
            "incarnation-b",
            2,
            1,
            expires_at,
        )
        await ownership_ref.set(encode_record(replacement))
        assert not await inbox.advance_input(
            conversation_key=conversation_key,
            input_id=envelope.input_id,
            owner=owner,
            expected=InputDeliveryState.DISPATCHING,
            new_state=InputDeliveryState.ACCEPTED,
        )
        assert await inbox.recover_dispatching(
            conversation_key=conversation_key,
            input_id=envelope.input_id,
            previous_owner_epoch=owner.epoch,
            dispatch_attempt_id="attempt-2",
        )
        assert await inbox.list_unfinished(conversation_key) == ()
    finally:
        client.close()


@pytest.mark.asyncio
async def test_conversation_updates_are_partial_versioned_and_set_owner_once():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")

    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    conversation_key = f"conversation-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    owner = OwnershipToken(
        conversation_key,
        "worker-a",
        "incarnation-a",
        1,
        1,
        now.replace(year=now.year + 1),
    )
    try:
        await inbox.accept_input(
            conversation_key=conversation_key,
            input_id="input-1",
            conversation_generation=1,
            kind="message",
            payload={"text": "hello"},
            created_at=now,
        )
        conversation_id = inbox._conversation_id(conversation_key)
        await (
            inbox._collection("ownership")
            .document(conversation_id)
            .set({**encode_record(owner), "phase": "active"})
        )

        assert await conversations.set_initial_owner_once(
            owner=owner, owner_user="user-1", owner_email="user@example.test"
        )
        assert not await conversations.set_initial_owner_once(
            owner=owner, owner_user="user-2", owner_email="other@example.test"
        )
        assert (
            await conversations.commit_turn_state(
                owner=owner,
                expected_version=2,
                patch={"turn_status": "running", "turn_id": "turn-1", "sdk_session_id": "sdk-1"},
            )
            == 3
        )

        progress_version = await conversations.commit_turn_state(
            owner=owner, expected_version=3, patch={"progress_ref": {"channel": "C1"}}
        )
        assert progress_version == 4
        current = await conversations.get_conversation(conversation_key)
        assert current is not None
        history_version = await conversations.commit_turn_state(
            owner=owner,
            expected_version=current["version"],
            patch={"history_uncertain": True},
        )
        assert history_version == 5

        conversation = (
            await inbox._collection("conversations").document(conversation_id).get()
        ).to_dict()
        assert conversation is not None
        assert conversation["next_input_seq"] == 2
        assert conversation["owner_user"] == "user-1"
        assert conversation["sdk_session_id"] == "sdk-1"
        assert conversation["turn_status"] == "running"
        assert conversation["turn_id"] == "turn-1"
        assert conversation["progress_ref"] == {"channel": "C1"}
        assert conversation["history_uncertain"] is True
    finally:
        client.close()


@pytest.mark.asyncio
async def test_reset_advances_generation_clears_session_and_rebinds_later_input():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    key = f"conversation-reset-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    owner = OwnershipToken(key, "worker-a", "incarnation-a", 1, 1, now + timedelta(hours=1))
    owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))
    try:
        reset = await inbox.accept_control(
            conversation_key=key,
            input_id="reset-1",
            conversation_generation=1,
            kind="reset_candidate",
            sender_id="user-1",
            payload={"text": "reset"},
            created_at=now,
        )
        later = await inbox.accept_input(
            conversation_key=key,
            input_id="after-reset",
            conversation_generation=1,
            kind="message",
            payload={"text": "keep me"},
            created_at=now,
        )
        old_notice = await inbox.accept_input(
            conversation_key=key,
            input_id="old-command-finished",
            conversation_generation=1,
            kind="command_finished",
            payload={"command_id": "command-old"},
            created_at=now,
        )
        await owner_ref.set({**encode_record(owner), "phase": "active"})
        conversation_ref = inbox._collection("conversations").document(inbox._conversation_id(key))
        await conversation_ref.update(
            {"turn_status": "idle", "sdk_session_id": "old-sdk", "guard_history": ["old"]}
        )
        assert await inbox.resolve_control_authorization(
            conversation_key=key, input_id=reset.input_id, authorized=True, now=now
        )
        actor_updates = []

        async def no_cancel_commands():
            return None

        async def update_actor(token):
            actor_updates.append(token)

        result = await DistributedResetCoordinator(
            inbox,
            conversations,
            SystemClock(),
            owner,
            stop=DistributedStopCoordinator(
                inbox, conversations, owner, cancel_commands=no_cancel_commands
            ),
            update_actor=update_actor,
            reset_seq=reset.seq,
        ).reset()
        assert result is not None
        new_owner = result.owner
        assert new_owner is not None
        assert (new_owner.conversation_generation, new_owner.epoch) == (2, 2)
        assert result.inputs_rebound == 1
        assert actor_updates == [new_owner]
        saved = await conversations.get_conversation(key)
        assert saved is not None
        assert saved["conversation_generation"] == 2
        assert saved["sdk_session_id"] is None
        assert saved["guard_history"] == []
        assert saved["stop_seq"] == 0
        rebound = await inbox.list_unfinished(key, after_seq=reset.seq)
        assert [(item.input_id, item.conversation_generation) for item in rebound] == [
            (later.input_id, 2)
        ]
        old_notice_ref = conversation_ref.collection("inputs").document(
            document_id(old_notice.input_id, prefix="input")
        )
        assert (await old_notice_ref.get()).get("delivery_state") == "suppressed"
    finally:
        client.close()


@pytest.mark.asyncio
async def test_expired_ownership_waits_for_stop_evidence_before_reallocation():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    allocator = FirestoreAllocator(client, collection_prefix=prefix)
    ownership = FirestoreOwnershipStore(client, collection_prefix=prefix)
    command_monitor = FirestoreCommandMonitorStore(client, collection_prefix=prefix)
    key = f"conversation-recovery-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    worker = WorkerRecord("worker-a", "incarnation-a", 1, 0, 0, False, now)
    await directory.register(worker)
    await inbox.accept_input(
        conversation_key=key,
        input_id="pending-input",
        conversation_generation=1,
        kind="message",
        payload={"text": "after recovery"},
        created_at=now,
    )
    await inbox.accept_input(
        conversation_key=key,
        input_id="dispatching-input",
        conversation_generation=1,
        kind="message",
        payload={"text": "delivery outcome unknown"},
        created_at=now,
    )
    held_completion_ref = (
        client.collection(f"{prefix}_conversations")
        .document(conversation_document_id(key))
        .collection("inputs")
        .document(document_id("held-completion", prefix="input"))
    )
    await held_completion_ref.set(
        {"input_id": "held-completion", "delivery_state": "held", "schema_version": 1}
    )
    try:
        old = await allocator.allocate(
            allocator_id="control-recovery",
            conversation_key=key,
            candidates=(worker,),
            now=now,
            allocator_lease_expires_at=now + timedelta(seconds=30),
            worker_lease_expires_at=now + timedelta(seconds=5),
        )
        assert old is not None
        assert await ownership.activate(old, now=now)
        command_collection = client.collection(f"{prefix}_commands")
        foreground_command_ref = command_collection.document(
            document_id("foreground-command", prefix="command")
        )
        background_command_ref = command_collection.document(
            document_id("background-command", prefix="command")
        )
        await foreground_command_ref.set(
            {
                "command_id": "foreground-command",
                "conversation_key": key,
                "conversation_generation": 1,
                "origin_epoch": old.epoch,
                "state": "running",
                "delivery_mode": "foreground",
                "schema_version": 1,
            }
        )
        await background_command_ref.set(
            {
                "command_id": "background-command",
                "conversation_key": key,
                "conversation_generation": 1,
                "sandbox_generation": 1,
                "origin_epoch": old.epoch,
                "state": "running",
                "delivery_mode": "background",
                "schema_version": 1,
            }
        )
        assert await inbox.advance_input(
            conversation_key=key,
            input_id="dispatching-input",
            owner=old,
            expected=InputDeliveryState.PENDING,
            new_state=InputDeliveryState.DISPATCHING,
            dispatch_attempt_id="dispatch-attempt-1",
        )
        expired_at = now + timedelta(seconds=6)
        assert await ownership.begin_recovery(old, now=expired_at)
        assert await ownership.mark_recovery_blocked(
            old, reason="worker_unreachable", now=expired_at
        )
        assert (
            await allocator.allocate(
                allocator_id="control-recovery",
                conversation_key=key,
                candidates=(worker,),
                now=expired_at,
                allocator_lease_expires_at=expired_at + timedelta(seconds=30),
                worker_lease_expires_at=expired_at + timedelta(seconds=60),
            )
            is None
        )
        assert await ownership.confirm_stopped(
            old,
            evidence_id="sdk-closed-ack-1",
            worker_incarnation="incarnation-a",
            now=expired_at + timedelta(seconds=1),
        )
        recovered_input = await (
            client.collection(f"{prefix}_conversations")
            .document(conversation_document_id(key))
            .collection("inputs")
            .document(document_id("dispatching-input", prefix="input"))
            .get()
        )
        assert recovered_input.get("delivery_state") == "uncertain"
        assert (await held_completion_ref.get()).get("delivery_state") == "held"
        assert (await foreground_command_ref.get()).get("delivery_mode") == "orphaned"
        assert (await background_command_ref.get()).get("delivery_mode") == "background"
        resumed_command = await command_monitor.get_command("background-command")
        assert resumed_command is not None
        assert resumed_command.get("command_id") == "background-command"
        assert resumed_command.get("state") == "running"
        assert resumed_command.get("sandbox_generation") == 1
        new = await allocator.allocate(
            allocator_id="control-recovery",
            conversation_key=key,
            candidates=(worker,),
            now=expired_at + timedelta(seconds=2),
            allocator_lease_expires_at=expired_at + timedelta(seconds=30),
            worker_lease_expires_at=expired_at + timedelta(seconds=60),
        )
        assert new is not None and new.epoch == old.epoch + 1
        assert not await inbox.advance_input(
            conversation_key=key,
            input_id="pending-input",
            owner=old,
            expected=InputDeliveryState.PENDING,
            new_state=InputDeliveryState.DISPATCHING,
            dispatch_attempt_id="stale-attempt",
        )
        current_worker = await directory._worker_ref(worker.worker_id).get()
        assert current_worker.get("reserved") == 1
        assert current_worker.get("active_sdk") == 0
    finally:
        client.close()


@pytest.mark.asyncio
async def test_worker_restart_replaces_incarnation_and_rejects_old_heartbeat():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")

    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    old = WorkerRecord(
        "worker-pod", worker_incarnation("pod-uid", "startup-1"), 4, 0, 0, False, now
    )
    restarted = WorkerRecord(
        "worker-pod",
        worker_incarnation("pod-uid", "startup-2"),
        4,
        0,
        0,
        False,
        now + timedelta(seconds=1),
    )
    try:
        assert await directory.register(old)
        assert await directory.register(restarted)
        assert not await directory.heartbeat(old)
        assert await directory.heartbeat(
            WorkerRecord(
                "worker-pod",
                restarted.incarnation,
                4,
                0,
                1,
                False,
                restarted.heartbeat_at + timedelta(seconds=1),
            )
        )
        available = await directory.list_available(now=now + timedelta(seconds=3))
        assert len(available) == 1
        assert available[0].incarnation == restarted.incarnation
        assert available[0].active_sdk == 1
    finally:
        client.close()


@pytest.mark.asyncio
async def test_worker_drain_deadline_survives_heartbeat_and_undrain_clears():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")

    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    record = WorkerRecord("worker-drain", "incarnation-1", 2, 0, 0, False, now)
    deadline = now + timedelta(minutes=5)
    try:
        assert await directory.register(record)
        draining = await directory.set_draining(
            worker_id=record.worker_id,
            incarnation=record.incarnation,
            draining=True,
            now=now,
            drain_deadline_at=deadline,
        )
        assert draining is not None and draining.drain_deadline_at == deadline
        assert await directory.heartbeat(
            WorkerRecord(
                record.worker_id,
                record.incarnation,
                record.capacity,
                0,
                1,
                False,
                now + timedelta(seconds=1),
            )
        )
        persisted = await directory.get_worker(record.worker_id)
        assert persisted is not None
        assert persisted.draining and persisted.drain_deadline_at == deadline
        undrained = await directory.set_draining(
            worker_id=record.worker_id,
            incarnation=record.incarnation,
            draining=False,
            now=now + timedelta(seconds=2),
        )
        assert undrained is not None
        assert not undrained.draining and undrained.drain_deadline_at is None
    finally:
        client.close()


@pytest.mark.asyncio
async def test_global_drain_blocks_new_worker_allocations_until_released():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    allocator = FirestoreAllocator(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    worker = WorkerRecord("worker-global-drain", "incarnation-1", 1, 0, 0, False, now)
    key = f"global-drain-{uuid.uuid4().hex}"
    try:
        await directory.register(worker)
        pending_input = await inbox.accept_input(
            conversation_key=key,
            input_id="pending",
            conversation_generation=1,
            kind="message",
            payload={"text": "remain queued while draining"},
            created_at=now,
        )
        await directory.set_cluster_draining(True)
        assert await directory.is_cluster_draining()
        assert (
            await allocator.allocate(
                allocator_id="control-global-drain",
                conversation_key=key,
                candidates=(worker,),
                now=now,
                allocator_lease_expires_at=now + timedelta(seconds=30),
                worker_lease_expires_at=now + timedelta(seconds=60),
            )
            is None
        )
        queued = await inbox.list_unfinished(key, after_seq=0, limit=10)
        assert [(item.input_id, item.delivery_state) for item in queued] == [
            (pending_input.input_id, InputDeliveryState.PENDING)
        ]
        await directory.set_cluster_draining(False)
        assert not await directory.is_cluster_draining()
        assert (
            await allocator.allocate(
                allocator_id="control-global-drain",
                conversation_key=key,
                candidates=(worker,),
                now=now + timedelta(seconds=1),
                allocator_lease_expires_at=now + timedelta(seconds=31),
                worker_lease_expires_at=now + timedelta(seconds=61),
            )
            is not None
        )
        assert await inbox.list_unfinished(key, after_seq=0, limit=10) == queued
    finally:
        client.close()


@pytest.mark.asyncio
async def test_concurrent_allocations_do_not_exceed_worker_capacity():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")

    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    allocator = FirestoreAllocator(client, collection_prefix=prefix)
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    now = datetime.now(UTC)
    worker = WorkerRecord("worker-1", "incarnation-1", 1, 0, 0, False, now)
    await directory.register(worker)
    await inbox.accept_input(
        conversation_key="conversation-a",
        input_id="input-a",
        conversation_generation=1,
        kind="message",
        payload={"text": "a"},
        created_at=now,
    )
    await inbox.accept_input(
        conversation_key="conversation-b",
        input_id="input-b",
        conversation_generation=1,
        kind="message",
        payload={"text": "b"},
        created_at=now,
    )
    candidate = WorkerRecord(
        worker.worker_id,
        worker.incarnation,
        worker.capacity,
        worker.reserved,
        worker.active_sdk,
        worker.draining,
        worker.heartbeat_at,
    )

    async def allocate(key: str):
        return await allocator.allocate(
            allocator_id=f"control-{key}",
            conversation_key=key,
            candidates=(candidate,),
            now=now,
            allocator_lease_expires_at=now + timedelta(seconds=30),
            worker_lease_expires_at=now + timedelta(seconds=60),
        )

    try:
        results = await asyncio.gather(allocate("conversation-a"), allocate("conversation-b"))
        assert sum(result is not None for result in results) == 1
        stored = (await directory._worker_ref(worker.worker_id).get()).to_dict()
        assert stored is not None
        assert stored["reserved"] == 1
        allocations = []
        for key in ("conversation-a", "conversation-b"):
            ref = inbox._collection("conversations").document(inbox._conversation_id(key))
            allocations.append((await ref.get()).get("allocation_state"))
        assert sorted(allocations) == ["assigned", "waiting"]
    finally:
        client.close()


@pytest.mark.asyncio
async def test_two_gateways_and_two_workers_route_one_event_without_slack_or_sdk():
    """Exercise local ingress -> Firestore -> competing Worker activation end to end."""
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")

    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    now = datetime.now(UTC)
    clock = SystemClock()
    inbox_a = FirestoreInboxStore(client, collection_prefix=prefix)
    inbox_b = FirestoreInboxStore(client, collection_prefix=prefix)
    conversation_a = "slack:T-local:C-local:100.1"
    conversation_b = "slack:T-local:C-local:200.1"

    async def normalize(envelope):
        event_id = envelope["payload"]["event_id"]
        conversation_key, input_id, message_ts = (
            (conversation_a, "C-local:100.2", "100.2")
            if event_id == "Ev-local-1"
            else (conversation_b, "C-local:200.2", "200.2")
        )
        return ReceivedSlackInput(
            conversation_key=conversation_key,
            input_id=input_id,
            kind="message",
            payload={"text": "run a local check"},
            workspace="W-local",
            team="T-local",
            channel="C-local",
            message_ts=message_ts,
            event_id=event_id,
            mark_participating=True,
        )

    gateway_a = SlackSocketGateway(inbox_a, clock, normalize)
    gateway_b = SlackSocketGateway(inbox_b, clock, normalize)
    acknowledgements = []

    async def ack(response):
        acknowledgements.append(response.envelope_id)

    async def deliver(gateway, envelope_id):
        envelope = {
            "type": "events_api",
            "envelope_id": envelope_id,
            "payload": {"event_id": "Ev-local-1" if envelope_id != "envelope-c" else "Ev-local-2"},
        }
        await gateway.handle_message(json.dumps(envelope), ack)

    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    allocator_a = FirestoreAllocator(client, collection_prefix=prefix)
    allocator_b = FirestoreAllocator(client, collection_prefix=prefix)
    ownership = FirestoreOwnershipStore(client, collection_prefix=prefix)
    workers = (
        WorkerRecord("worker-a", "pod-a:boot-1", 1, 0, 0, False, now),
        WorkerRecord("worker-b", "pod-b:boot-1", 1, 0, 0, False, now),
    )

    class Actor:
        def __init__(self, token):
            self.token = token
            self.started = 0

        async def start(self):
            self.started += 1

        async def update_ownership(self, token):
            self.token = token

        async def aclose(self):
            return None

    spawned = []

    def make_actor(token):
        actor = Actor(token)
        spawned.append(actor)
        return actor

    def no_watch(_on_change, _on_error):
        class Handle:
            def unsubscribe(self):
                return None

        return Handle()

    worker_runners = []
    try:
        await asyncio.gather(
            directory.register(workers[0]),
            directory.register(workers[1]),
            deliver(gateway_a, "envelope-a"),
            deliver(gateway_b, "envelope-b"),
            deliver(gateway_a, "envelope-c"),
        )
        assert sorted(acknowledgements) == ["envelope-a", "envelope-b", "envelope-c"]
        records_a = await inbox_a.list_unfinished(conversation_a)
        records_b = await inbox_a.list_unfinished(conversation_b)
        assert len(records_a) == len(records_b) == 1
        assert records_a[0].input_id == "C-local:100.2"
        assert records_b[0].input_id == "C-local:200.2"

        candidates = tuple(await directory.list_available(now=now))
        assert len(candidates) == 2

        async def allocate(allocator, allocator_id, key):
            return await allocator.allocate(
                allocator_id=allocator_id,
                conversation_key=key,
                candidates=candidates,
                now=now,
                allocator_lease_expires_at=now + timedelta(seconds=30),
                worker_lease_expires_at=now + timedelta(seconds=60),
            )

        assignments = await asyncio.gather(
            allocate(allocator_a, "control-a", conversation_a),
            allocate(allocator_b, "control-b", conversation_a),
            allocate(allocator_a, "control-a", conversation_b),
            allocate(allocator_b, "control-b", conversation_b),
        )
        assert sum(assignment is not None for assignment in assignments) == 2
        assert {assignment.worker_id for assignment in assignments if assignment} == {
            "worker-a",
            "worker-b",
        }

        worker_runners = [
            DistributedWorker(
                ownership,
                clock,
                worker_id=record.worker_id,
                worker_incarnation=record.incarnation,
                actor_factory=make_actor,
                subscribe=no_watch,
            )
            for record in workers
        ]
        await asyncio.gather(*(worker.run_once() for worker in worker_runners))
        assert len(spawned) == 2
        assert all(actor.started == 1 for actor in spawned)
        assert {actor.token.conversation_key for actor in spawned} == {
            conversation_a,
            conversation_b,
        }
        assert len({actor.token.worker_id for actor in spawned}) == 2
        for key in (conversation_a, conversation_b):
            owner_ref, _ = ownership._refs(key)
            owner_snapshot = await owner_ref.get()
            assert owner_snapshot.get("phase") == "active"

        command_id = f"command-{uuid.uuid4().hex}"
        command_ref = client.collection(f"{prefix}_commands").document(
            document_id(command_id, prefix="command")
        )
        await command_ref.set(
            {
                "command_id": command_id,
                "conversation_key": conversation_a,
                "conversation_generation": 1,
                "sandbox_generation": 1,
                "state": "running",
                "delivery_mode": "background",
                "notification_state": "none",
                "next_attempt_at": now,
            }
        )
        monitor_store_a = FirestoreCommandMonitorStore(client, collection_prefix=prefix)
        monitor_store_b = FirestoreCommandMonitorStore(client, collection_prefix=prefix)

        class Runtime:
            def __init__(self):
                self.status_calls = []
                self.entered = asyncio.Event()
                self.release = asyncio.Event()

            async def command_status(self, key, requested_command_id):
                self.status_calls.append((key, requested_command_id))
                self.entered.set()
                await self.release.wait()
                return CommandResult(requested_command_id, "completed", exit_code=0)

        runtime = Runtime()
        monitor_a = CommandMonitor(
            monitor_store_a, runtime, clock, monitor_id="monitor-a", retry_seconds=1
        )
        monitor_b = CommandMonitor(
            monitor_store_b, runtime, clock, monitor_id="monitor-b", retry_seconds=1
        )
        first_monitor = asyncio.create_task(monitor_a.poll_once())
        await runtime.entered.wait()
        second_monitor = await monitor_b.poll_once()
        runtime.release.set()
        first_monitor_result = await first_monitor
        assert first_monitor_result == 1
        assert second_monitor == 0
        assert runtime.status_calls == [(conversation_a, command_id)]
        saved_command = await monitor_store_a.get_command(command_id)
        assert saved_command["terminal_delivery"] == "notify"
        finished_inputs = [
            item
            for item in await inbox_a.list_unfinished(conversation_a)
            if item.kind == "command_finished"
        ]
        assert len(finished_inputs) == 1

        watch_source = FakeWatchSource()
        watched_ids = []
        first_seen = asyncio.Event()
        recovered = asyncio.Event()

        async def read_watched_inputs():
            return await inbox_a.list_unfinished(conversation_b)

        async def consume_watched_input(item):
            watched_ids.append(item.input_id)
            if item.input_id == "C-local:200.2":
                first_seen.set()
            if item.input_id == "C-local:200.3":
                recovered.set()

        watch = FirestoreWatchAdapter(
            subscribe=watch_source.subscribe,
            read_pending=read_watched_inputs,
            consume=consume_watched_input,
            item_key=lambda item: item.input_id,
            item_version=lambda item: item.delivery_state,
            retry_seconds=0,
            clock=clock,
        )
        watch_task = asyncio.create_task(watch.run())
        try:
            await first_seen.wait()
            await inbox_a.accept_input(
                conversation_key=conversation_b,
                input_id="C-local:200.3",
                conversation_generation=1,
                kind="message",
                payload={"text": "recovered after watch reconnect"},
                created_at=clock.now(),
            )
            await watch_source.fail_from_thread(ConnectionError("emulator watch reset"))
            await watch_source.wait_for_subscriptions(2)
            await recovered.wait()
            assert watched_ids == ["C-local:200.2", "C-local:200.3"]
        finally:
            watch_task.cancel()
            await asyncio.gather(watch_task, return_exceptions=True)
    finally:
        await asyncio.gather(gateway_a.aclose(), gateway_b.aclose())
        await asyncio.gather(*(worker.aclose() for worker in worker_runners))
        client.close()


@pytest.mark.asyncio
async def test_ownership_activation_renewal_and_release_are_fenced():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")

    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    directory = FirestoreWorkerDirectory(client, collection_prefix=prefix)
    allocator = FirestoreAllocator(client, collection_prefix=prefix)
    ownership = FirestoreOwnershipStore(client, collection_prefix=prefix)
    outbox = FirestoreOutboxStore(client, collection_prefix=prefix)
    operations = FirestoreServiceOperationStore(client, collection_prefix=prefix)
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversation_key = f"conversation-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    initial_expiry = now + timedelta(seconds=60)
    worker = WorkerRecord("worker-1", "incarnation-1", 1, 0, 0, False, now)
    await directory.register(worker)
    await inbox.accept_input(
        conversation_key=conversation_key,
        input_id="input-1",
        conversation_generation=1,
        kind="message",
        payload={"text": "hello"},
        created_at=now,
    )
    conversation_id = inbox._conversation_id(conversation_key)
    sandbox_key_hash = "a" * 64
    await (
        inbox._collection("sandboxes")
        .document(sandbox_key_hash)
        .set(
            {
                "key_hash": sandbox_key_hash,
                "generation": 1,
                "state": "ready",
                "schema_version": 1,
                "resource_version": 1,
            }
        )
    )
    try:
        assigned = await allocator.allocate(
            allocator_id="control-1",
            conversation_key=conversation_key,
            candidates=(worker,),
            now=now,
            allocator_lease_expires_at=now + timedelta(seconds=30),
            worker_lease_expires_at=initial_expiry,
        )
        assert assigned is not None
        assert not await ownership.activate(assigned, now=initial_expiry)

        active_at = now + timedelta(seconds=1)
        assert await ownership.activate(assigned, now=active_at)
        assert not await ownership.activate(assigned, now=active_at)
        renewed = await ownership.renew(
            assigned,
            now=active_at,
            lease_expires_at=now + timedelta(seconds=120),
        )
        assert renewed is not None
        assert renewed.epoch == assigned.epoch
        assert renewed.expires_at > assigned.expires_at
        assert await outbox.put_sanitized(
            output_id="output-1",
            conversation_key=conversation_key,
            conversation_generation=1,
            sequence=1,
            owner=renewed,
            payload={"kind": "message", "text": "sanitized"},
            now=active_at,
            created_at=active_at,
        )
        assert await outbox.put_sanitized(
            output_id="output-2",
            conversation_key=conversation_key,
            conversation_generation=1,
            sequence=2,
            owner=renewed,
            payload={"kind": "message", "text": "second"},
            now=active_at,
            created_at=active_at,
        )
        left, right = await asyncio.gather(
            outbox.claim_next(
                conversation_key=conversation_key,
                sender_id="sender-a",
                now=active_at,
                lease_expires_at=active_at + timedelta(seconds=30),
            ),
            outbox.claim_next(
                conversation_key=conversation_key,
                sender_id="sender-b",
                now=active_at,
                lease_expires_at=active_at + timedelta(seconds=30),
            ),
            return_exceptions=True,
        )
        if isinstance(left, Exception):
            if not isinstance(left, FirestoreStoreError) or left.status is not None:
                raise left
            left = await outbox.claim_next(
                conversation_key=conversation_key,
                sender_id="sender-a",
                now=active_at,
                lease_expires_at=active_at + timedelta(seconds=30),
            )
        if isinstance(right, Exception):
            if not isinstance(right, FirestoreStoreError) or right.status is not None:
                raise right
            right = await outbox.claim_next(
                conversation_key=conversation_key,
                sender_id="sender-b",
                now=active_at,
                lease_expires_at=active_at + timedelta(seconds=30),
            )
        claimed = left or right
        assert claimed is not None and claimed["output_id"] == "output-1"
        assert (left is None) != (right is None)
        assert (
            await outbox.claim_next(
                conversation_key=conversation_key,
                sender_id="sender-c",
                now=active_at,
                lease_expires_at=active_at + timedelta(seconds=30),
            )
            is None
        )
        assert await outbox.mark_delivery(
            output_id="output-1",
            expected=OutputDeliveryState.DISPATCHING,
            new_state=OutputDeliveryState.DELIVERED,
            attempt_id=claimed["attempt_id"],
            slack_ref={"channel": "C1", "ts": "1.0"},
            updated_at=active_at,
        )
        second_claim = await outbox.claim_next(
            conversation_key=conversation_key,
            sender_id="sender-c",
            now=active_at,
            lease_expires_at=active_at + timedelta(seconds=30),
        )
        assert second_claim is not None and second_claim["output_id"] == "output-2"
        assert await outbox.mark_delivery(
            output_id="output-2",
            expected=OutputDeliveryState.DISPATCHING,
            new_state=OutputDeliveryState.DELIVERED,
            attempt_id=second_claim["attempt_id"],
            slack_ref={"channel": "C1", "ts": "2.0"},
            updated_at=active_at,
        )
        assert await outbox.put_sanitized(
            output_id="output-3",
            conversation_key=conversation_key,
            conversation_generation=1,
            sequence=3,
            origin_seq=2,
            owner=renewed,
            payload={"kind": "message", "text": "later turn"},
            now=active_at,
            created_at=active_at,
        )
        third_claim = await outbox.claim_next(
            conversation_key=conversation_key,
            sender_id="sender-d",
            now=active_at,
            lease_expires_at=active_at + timedelta(seconds=30),
        )
        assert third_claim is not None and third_claim["output_id"] == "output-3"
        conversation_ref = inbox._collection("conversations").document(conversation_id)
        await conversation_ref.update({"stop_seq": 2})
        assert not await outbox.revalidate_claim(
            output_id="output-3", attempt_id=third_claim["attempt_id"], now=active_at
        )
        assert (
            await client.collection(f"{prefix}_outbox")
            .document(document_id("output-3", prefix="output"))
            .get()
        ).get("status") == "suppressed"
        await conversation_ref.update({"stop_seq": 1})
        assert not await outbox.put_sanitized(
            output_id="output-stopped-turn",
            conversation_key=conversation_key,
            conversation_generation=1,
            sequence=3,
            origin_seq=1,
            owner=renewed,
            payload={"kind": "message", "text": "stale turn"},
            now=active_at,
            created_at=active_at,
        )
        await conversation_ref.update({"stop_seq": 0})
        operation = {
            "operation_id": "file-write-1",
            "operation_kind": "file_write",
            "request_hash": "hash-1",
            "conversation_key": conversation_key,
            "conversation_generation": 1,
            "sandbox_generation": 1,
            "sandbox_key_hash": sandbox_key_hash,
            "turn_id": "turn-1",
            "origin_seq": 1,
            "owner": renewed,
            "payload": {"path": "notes.txt", "content": "safe"},
            "now": active_at,
            "created_at": active_at,
        }
        assert await operations.accept_intent(**operation)
        assert await operations.is_dispatch_allowed("file-write-1", renewed, now=active_at)
        assert not await operations.accept_intent(**operation)
        assert (await operations.get_operation("file-write-1"))["state"] == "accepted"
        second_operation = {
            **operation,
            "operation_id": "file-write-2",
            "request_hash": "hash-2",
        }
        assert await operations.accept_intent(**second_operation)
        page, cursor = await operations.list_for_stop(
            conversation_key=conversation_key,
            conversation_generation=1,
            stop_seq=1,
            limit=1,
        )
        assert len(page) == 1
        assert cursor is not None
        final_page, cursor = await operations.list_for_stop(
            conversation_key=conversation_key,
            conversation_generation=1,
            stop_seq=1,
            limit=1,
            cursor=cursor,
        )
        assert len(final_page) == 1
        assert cursor is None
        command_operation = {
            **operation,
            "operation_id": "command-start-1",
            "operation_kind": "command_start",
            "request_hash": "command-hash-1",
            "payload": {"command": "echo ok", "cwd": ".", "timeout_seconds": 30},
        }
        assert await operations.accept_intent(**command_operation)
        command_record = (
            await operations.client.collection(f"{prefix}_commands")
            .document(document_id("command-start-1", prefix="command"))
            .get()
        )
        assert command_record.exists
        assert command_record.to_dict()["state"] == "intent"
        assert command_record.to_dict()["origin_epoch"] == renewed.epoch

        conversation_ref = inbox._collection("conversations").document(conversation_id)
        await conversation_ref.update({"stop_seq": 1})
        assert not await operations.is_dispatch_allowed("file-write-1", renewed, now=active_at)
        rejected_after_stop = {
            **operation,
            "operation_id": "file-write-after-stop",
            "request_hash": "hash-after-stop",
        }
        assert not await operations.accept_intent(**rejected_after_stop)
        assert (
            len(
                (
                    await operations.list_for_stop(
                        conversation_key=conversation_key,
                        conversation_generation=1,
                        stop_seq=1,
                    )
                )[0]
            )
            == 3
        )
        assert (
            await ownership.renew(
                assigned,
                now=active_at,
                lease_expires_at=now + timedelta(seconds=180),
            )
            is None
        )
        assert not await ownership.release(assigned, now=active_at)

        assert await ownership.release(renewed, now=active_at)
        assert not await ownership.release(renewed, now=active_at)
        assert not await outbox.put_sanitized(
            output_id="output-stale",
            conversation_key=conversation_key,
            conversation_generation=1,
            sequence=2,
            owner=renewed,
            payload={"kind": "message", "text": "late"},
            now=active_at,
            created_at=active_at,
        )
        assert not await operations.accept_intent(**operation)
        assert await ownership.get(conversation_key) is None
        worker_data = (await directory._worker_ref(worker.worker_id).get()).to_dict()
        assert worker_data is not None
        assert worker_data["reserved"] == 0
        assert worker_data["active_sdk"] == 0
        conversation_ref = inbox._collection("conversations").document(
            inbox._conversation_id(conversation_key)
        )
        assert (await conversation_ref.get()).get("allocation_state") == "waiting"

        second_assignment = await allocator.allocate(
            allocator_id="control-1",
            conversation_key=conversation_key,
            candidates=(worker,),
            now=active_at + timedelta(seconds=1),
            allocator_lease_expires_at=now + timedelta(seconds=90),
            worker_lease_expires_at=now + timedelta(seconds=180),
        )
        assert second_assignment is not None
        assert second_assignment.epoch == assigned.epoch + 1
        assert await ownership.activate(second_assignment, now=active_at + timedelta(seconds=1))
        assert await inbox.advance_input(
            conversation_key=conversation_key,
            input_id="input-1",
            owner=second_assignment,
            expected=InputDeliveryState.PENDING,
            new_state=InputDeliveryState.BLOCKED,
        )
        release_time = active_at + timedelta(seconds=1)
        released, raced_input = await asyncio.gather(
            ownership.release(second_assignment, now=release_time),
            inbox.accept_input(
                conversation_key=conversation_key,
                input_id="input-2",
                conversation_generation=1,
                kind="message",
                payload={"text": "arrived during release"},
                created_at=release_time,
            ),
        )
        assert released
        assert raced_input.input_id == "input-2"
        assert (await conversation_ref.get()).get("allocation_state") == "waiting"
        assert [item.input_id for item in await inbox.list_unfinished(conversation_key)] == [
            "input-2"
        ]

        third_assignment = await allocator.allocate(
            allocator_id="control-1",
            conversation_key=conversation_key,
            candidates=(worker,),
            now=release_time + timedelta(seconds=1),
            allocator_lease_expires_at=now + timedelta(seconds=120),
            worker_lease_expires_at=now + timedelta(seconds=240),
        )
        assert third_assignment is not None
        assert third_assignment.epoch == second_assignment.epoch + 1
        assert await ownership.activate(third_assignment, now=release_time + timedelta(seconds=1))
        race_time = release_time + timedelta(seconds=1)
        racing_operation = {
            **operation,
            "operation_id": "file-write-racing-release",
            "request_hash": "hash-race",
            "turn_id": "turn-2",
            "origin_seq": 2,
            "owner": third_assignment,
            "now": race_time,
            "created_at": race_time,
        }
        released, accepted_during_release = await asyncio.gather(
            ownership.release(third_assignment, now=race_time),
            operations.accept_intent(**racing_operation),
        )
        assert released
        raced_record = await operations.get_operation("file-write-racing-release")
        assert (raced_record is not None) is accepted_during_release
        if raced_record is not None:
            assert raced_record["origin_epoch"] == third_assignment.epoch

        # Even with the accepted operation, the still-pending input keeps allocation waiting.
        assert (await conversation_ref.get()).get("allocation_state") == "waiting"
        idle_assignment = await allocator.allocate(
            allocator_id="control-1",
            conversation_key=conversation_key,
            candidates=(worker,),
            now=race_time + timedelta(seconds=1),
            allocator_lease_expires_at=now + timedelta(seconds=150),
            worker_lease_expires_at=now + timedelta(seconds=300),
        )
        assert idle_assignment is not None
        assert await ownership.activate(idle_assignment, now=race_time + timedelta(seconds=1))
        assert await inbox.advance_input(
            conversation_key=conversation_key,
            input_id="input-2",
            owner=idle_assignment,
            expected=InputDeliveryState.PENDING,
            new_state=InputDeliveryState.BLOCKED,
        )
        assert await ownership.release(idle_assignment, now=race_time + timedelta(seconds=1))
        assert (await conversation_ref.get()).get("allocation_state") == "idle"
    finally:
        client.close()


@pytest.mark.asyncio
async def test_sender_enrichment_ignores_inputs_of_another_prefix():
    emulator_host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    project = f"silk-agent-test-{uuid.uuid4().hex[:12]}"
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=project), emulator_host=emulator_host
    )
    mine = FirestoreInboxStore(client, collection_prefix=f"mine-{uuid.uuid4().hex[:8]}")
    other = FirestoreInboxStore(client, collection_prefix=f"other-{uuid.uuid4().hex[:8]}")
    now = datetime.now(UTC)
    try:
        for store, key in ((mine, "mine-conversation"), (other, "other-conversation")):
            await store.accept_received(
                conversation_key=key,
                input_id="C1:100.1",
                conversation_generation=1,
                kind="message",
                payload={"text": "hello", "user": "U1"},
                created_at=now,
                workspace="T1",
                team="T1",
                channel="C1",
                message_ts="100.1",
                event_id=f"event-{key}",
            )
        # The collection group spans every prefix; each store sees only its own inputs.
        assert [r["conversation_key"] for r in await mine.list_sender_enrichment_due(now=now)] == [
            "mine-conversation"
        ]
        assert [r["conversation_key"] for r in await other.list_sender_enrichment_due(now=now)] == [
            "other-conversation"
        ]
    finally:
        client.close()
