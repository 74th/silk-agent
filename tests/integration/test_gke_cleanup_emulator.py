import os
import uuid

import pytest

from silk_agent.config import DistributedExecutionConfig
from silk_agent.stores.firestore.client import create_firestore_client
from tests.gke.cleanup import delete_records_of_conversation

pytestmark = pytest.mark.integration


async def test_the_records_of_a_conversation_go_whatever_their_command_ids_are():
    host = os.environ.get("SILK_AGENT_FIRESTORE_EMULATOR_HOST")
    if not host:
        pytest.skip("set SILK_AGENT_FIRESTORE_EMULATOR_HOST to run the Firestore emulator test")
    client = create_firestore_client(
        DistributedExecutionConfig(firestore_project=f"silk-agent-test-{uuid.uuid4().hex[:12]}"),
        emulator_host=host,
    )
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    mine, other = "distributed-validation:mine", "distributed-validation:other"
    try:
        # Records of a start whose response was lost: ids the test never learned.
        for name in ("commands", "sandbox_operations"):
            for key in (mine, mine, other):
                await client.collection(f"{prefix}_{name}").add({"conversation_key": key})
        assert await delete_records_of_conversation(client, prefix, mine) == 4
        for name in ("commands", "sandbox_operations"):
            left = [
                doc.to_dict()["conversation_key"]
                async for doc in client.collection(f"{prefix}_{name}").stream()
            ]
            assert left == [other]  # another run's records are not touched
    finally:
        client.close()
