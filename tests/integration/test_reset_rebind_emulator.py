import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.config import DistributedExecutionConfig
from silk_agent.core.clock import SystemClock
from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.distributed.reset import DistributedResetCoordinator
from silk_agent.distributed.stop import DistributedStopCoordinator
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.codec import encode_record
from silk_agent.stores.firestore.conversations import FirestoreConversationStore
from silk_agent.stores.firestore.inbox import FirestoreInboxStore

pytestmark = pytest.mark.integration


async def test_a_reset_that_stops_before_the_rebind_is_finished_by_the_next_owner():
    host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=f"silk-agent-test-{uuid.uuid4().hex[:12]}"),
        emulator_host=host,
    )
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    inbox = FirestoreInboxStore(client, collection_prefix=prefix)
    conversations = FirestoreConversationStore(client, collection_prefix=prefix)
    key = f"conversation-reset-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    owner = OwnershipToken(key, "worker-a", "incarnation-a", 1, 1, now + timedelta(hours=1))
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
        owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))
        await owner_ref.set({**encode_record(owner), "phase": "active"})
        conversation_ref = inbox._collection("conversations").document(inbox._conversation_id(key))
        await conversation_ref.update({"turn_status": "idle"})
        assert await inbox.resolve_control_authorization(
            conversation_key=key, input_id=reset.input_id, authorized=True, now=now
        )

        async def no_cancel_commands():
            return None

        async def update_actor(_token):
            return None

        real_rebind = inbox.rebind_after_reset

        async def worker_dies(**_kwargs):
            raise RuntimeError("worker stopped between the new generation and the rebind")

        inbox.rebind_after_reset = worker_dies
        coordinator = DistributedResetCoordinator(
            inbox,
            conversations,
            SystemClock(),
            owner,
            stop=DistributedStopCoordinator(
                inbox, conversations, owner, cancel_commands=no_cancel_commands
            ),
            update_actor=update_actor,
            reset_seq=reset.seq,
        )
        with pytest.raises(RuntimeError):
            await coordinator.reset()

        saved = (await conversation_ref.get()).to_dict()
        assert saved["conversation_generation"] == 2
        assert saved["rebind_pending"] == {"previous_generation": 1, "reset_seq": reset.seq}
        assert [e.conversation_generation for e in await inbox.list_unfinished(key)] == [1]
        assert await inbox.pending_rebind(key) == (1, reset.seq)

        # The next owner (same epoch + 1 as complete_reset issued) finishes the rebind.
        new_owner = OwnershipToken(key, "worker-b", "incarnation-b", 2, 2, now + timedelta(hours=1))
        await owner_ref.set({**encode_record(new_owner), "phase": "active"})
        inbox.rebind_after_reset = real_rebind
        previous, seq = await inbox.pending_rebind(key)
        rebound = await inbox.rebind_after_reset(
            conversation_key=key, previous_generation=previous, reset_seq=seq, owner=new_owner
        )
        assert rebound == 1
        moved = await inbox.list_unfinished(key, after_seq=reset.seq)
        assert [(e.input_id, e.conversation_generation) for e in moved] == [(later.input_id, 2)]
        assert await inbox.pending_rebind(key) is None
        assert "rebind_pending" not in (await conversation_ref.get()).to_dict()
    finally:
        client.close()


async def _half_done_reset(prefix_suffix):
    """Generation 2 is in place, two inputs still carry generation 1, the marker is set."""
    host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=f"silk-agent-test-{uuid.uuid4().hex[:12]}"),
        emulator_host=host,
    )
    inbox = FirestoreInboxStore(client, collection_prefix=f"test-{uuid.uuid4().hex[:12]}")
    key = f"conversation-{prefix_suffix}-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    reset = await inbox.accept_control(
        conversation_key=key,
        input_id="reset-1",
        conversation_generation=1,
        kind="reset_candidate",
        sender_id="user-1",
        payload={"text": "reset"},
        created_at=now,
    )
    inputs = [
        await inbox.accept_input(
            conversation_key=key,
            input_id=f"after-{n}",
            conversation_generation=1,
            kind="message",
            payload={"text": f"message {n}"},
            created_at=now,
        )
        for n in range(2)
    ]
    conversation_ref = inbox._collection("conversations").document(inbox._conversation_id(key))
    await conversation_ref.update(
        {
            "conversation_generation": 2,
            "rebind_pending": {"previous_generation": 1, "reset_seq": reset.seq},
        }
    )
    owner = OwnershipToken(key, "worker-a", "incarnation-a", 2, 2, now + timedelta(hours=1))
    owner_ref = inbox._collection("ownership").document(inbox._conversation_id(key))
    await owner_ref.set({**encode_record(owner), "phase": "active"})
    return client, inbox, key, now, reset, inputs, owner, owner_ref, conversation_ref


async def test_a_lease_renewal_during_the_rebind_does_not_lose_inputs_or_the_marker():
    (
        client,
        inbox,
        key,
        now,
        reset,
        inputs,
        owner,
        owner_ref,
        conversation_ref,
    ) = await _half_done_reset("renewed")
    try:
        # The Worker renewed its lease: same holder, later expiry. Its token in the rebind
        # call still carries the old expiry.
        renewed = OwnershipToken(
            key, owner.worker_id, owner.worker_incarnation, owner.epoch, 2, now + timedelta(hours=2)
        )
        await owner_ref.set({**encode_record(renewed), "phase": "active"})
        rebound = await inbox.rebind_after_reset(
            conversation_key=key, previous_generation=1, reset_seq=reset.seq, owner=owner
        )
        assert rebound == 2
        assert {
            e.conversation_generation for e in await inbox.list_unfinished(key, after_seq=reset.seq)
        } == {2}
        assert await inbox.pending_rebind(key) is None
    finally:
        client.close()


async def test_when_the_owner_changed_the_marker_stays_for_the_next_owner():
    (
        client,
        inbox,
        key,
        now,
        reset,
        inputs,
        owner,
        owner_ref,
        conversation_ref,
    ) = await _half_done_reset("taken")
    try:
        taken = OwnershipToken(key, "worker-b", "incarnation-b", 3, 2, now + timedelta(hours=1))
        await owner_ref.set({**encode_record(taken), "phase": "active"})
        rebound = await inbox.rebind_after_reset(
            conversation_key=key, previous_generation=1, reset_seq=reset.seq, owner=owner
        )
        assert rebound == 0  # this owner is fenced out: nothing moved...
        assert await inbox.pending_rebind(key) == (1, reset.seq)  # ...and the job is not lost
        # The new owner finishes it.
        moved = await inbox.rebind_after_reset(
            conversation_key=key, previous_generation=1, reset_seq=reset.seq, owner=taken
        )
        assert moved == 2 and await inbox.pending_rebind(key) is None
    finally:
        client.close()
