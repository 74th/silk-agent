import os
import uuid

import pytest

from silk_agent.config import DistributedExecutionConfig
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.ids import document_id
from silk_agent.stores.firestore.outbox import FirestoreOutboxStore

pytestmark = pytest.mark.integration


async def test_a_conversation_with_many_waiting_outputs_does_not_starve_the_others():
    host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=f"silk-agent-test-{uuid.uuid4().hex[:12]}"),
        emulator_host=host,
    )
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    outbox = FirestoreOutboxStore(client, collection_prefix=prefix)
    collection = client.collection(f"{prefix}_outbox")
    try:
        # Conversation "stuck" has a head output of unknown outcome and 250 more waiting.
        for number in range(250):
            output_id = f"stuck-{number}"
            await collection.document(document_id(output_id, prefix="output")).set(
                {"output_id": output_id, "conversation_key": "stuck", "status": "pending"}
            )
        for name in ("healthy-a", "healthy-b"):
            await collection.document(document_id(f"{name}-1", prefix="output")).set(
                {"output_id": f"{name}-1", "conversation_key": name, "status": "pending"}
            )

        seen: set[str] = set()
        cursor = None
        for _ in range(10):
            keys, cursor = await outbox.list_pending_page(limit=100, after=cursor)
            seen.update(keys)
            if {"healthy-a", "healthy-b"} <= seen:
                break
        assert {"healthy-a", "healthy-b"} <= seen
        # At the end of the data the cursor is None: the caller starts over.
        cursor = None
        for _ in range(10):
            _keys, cursor = await outbox.list_pending_page(limit=100, after=cursor)
            if cursor is None:
                break
        assert cursor is None
    finally:
        client.close()


async def test_a_failed_output_leaves_the_listing_and_gets_a_ttl():
    from datetime import UTC, datetime

    from silk_agent.core.distributed_models import OutputDeliveryState

    host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=f"silk-agent-test-{uuid.uuid4().hex[:12]}"),
        emulator_host=host,
    )
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    outbox = FirestoreOutboxStore(client, collection_prefix=prefix)
    ref = client.collection(f"{prefix}_outbox").document(document_id("out-1", prefix="output"))
    try:
        await ref.set(
            {
                "output_id": "out-1",
                "conversation_key": "room",
                "status": "dispatching",
                "attempt_id": "attempt-1",
            }
        )
        assert await outbox.mark_delivery(
            output_id="out-1",
            expected=OutputDeliveryState.DISPATCHING,
            new_state=OutputDeliveryState.FAILED,
            attempt_id="attempt-1",
            updated_at=datetime.now(UTC),
        )
        saved = (await ref.get()).to_dict()
        assert saved["status"] == "failed" and saved.get("expire_at") is not None
        keys, _ = await outbox.list_pending_page(limit=10)
        assert keys == ()
    finally:
        client.close()
