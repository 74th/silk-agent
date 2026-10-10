"""SessionStateStore against the real Agent Platform Sessions. Run with `-m integration`."""

import uuid
from datetime import UTC, datetime

import pytest

from silk_agent.core.ports import ConversationState
from silk_agent.stores.agent_platform import AgentPlatformSessions
from silk_agent.stores.state import AgentPlatformStateStore

pytestmark = pytest.mark.integration


async def test_save_load_and_list_running(sessions_api: AgentPlatformSessions):
    store = AgentPlatformStateStore(sessions_api)
    key = f"test:{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    await store.put(
        ConversationState(
            key,
            status="running",
            turn_id="t1",
            bot_instance_id="live-test",
            progress_ref={"ts": "1.2"},
            updated_at=now,
        )
    )
    fresh = AgentPlatformStateStore(sessions_api)  # no cache: reads from the service
    got = await fresh.get(key)
    assert got is not None and got.status == "running" and got.progress_ref == {"ts": "1.2"}
    assert key in [s.conversation_key for s in await fresh.list_running()]
    await store.put(ConversationState(key, status="idle", updated_at=now))
    assert key not in [
        s.conversation_key for s in await AgentPlatformStateStore(sessions_api).list_running()
    ]
