import pytest

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.ports import ConversationState
from silk_agent.stores.firestore import session_state as module
from silk_agent.stores.firestore.session_state import (
    FirestoreSessionStateStore,
    OwnershipLostError,
    StateConflictError,
)

KEY = "slack:T:C:1"


class FakeConversations:
    def __init__(self, record=None, commits=None, holds=True):
        self.record = record if record is not None else {"version": 1}
        self.commits = list(commits or [])  # one result per attempt; None = lost the race
        self.holds = holds
        self.patches = []

    async def get_conversation(self, key):
        return self.record

    async def commit_turn_state(self, *, owner, expected_version, patch):
        self.patches.append(dict(patch))
        return self.commits.pop(0) if self.commits else expected_version + 1

    async def holds_ownership(self, owner):
        return self.holds


def make_store(conversations):
    from datetime import UTC, datetime

    owner = OwnershipToken(KEY, "worker", "inc", 1, 1, datetime(2030, 1, 1, tzinfo=UTC))
    return FirestoreSessionStateStore(conversations, lambda key: owner)


async def test_only_the_fields_that_changed_since_the_read_are_written():
    conversations = FakeConversations(
        {
            "version": 7,
            "sdk_session_id": "sdk-1",
            "turn_status": "running",
            "turn_id": "turn-1",
            "running_command_ids": ["a"],
        }
    )
    store = make_store(conversations)
    state = await store.get(KEY)
    # Meanwhile the turn finished (another writer set turn_status idle); this caller only
    # tracks one more command and must not write the stale "running" back.
    state.running_command_ids = [*state.running_command_ids, "b"]
    await store.put(state)
    assert conversations.patches == [{"running_command_ids": ["a", "b"]}]


async def test_a_state_that_was_never_read_is_written_whole_and_an_unchanged_one_not_at_all():
    conversations = FakeConversations()
    store = make_store(conversations)
    await store.put(ConversationState(KEY, agent_session_id="sdk-9", status="running"))
    assert set(conversations.patches[0]) >= {"sdk_session_id", "turn_status", "turn_id"}
    state = await store.get(KEY)
    conversations.patches.clear()
    await store.put(state)  # nothing changed since the read
    assert conversations.patches == []


async def test_losing_every_race_for_the_version_is_a_conflict_not_a_fence(monkeypatch):
    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(module.asyncio, "sleep", no_sleep)
    conversations = FakeConversations(commits=[None] * 6, holds=True)
    store = make_store(conversations)
    with pytest.raises(StateConflictError):
        await store.put(ConversationState(KEY, status="idle"))
    assert len(conversations.patches) == 6


async def test_a_writer_that_lost_the_ownership_is_fenced(monkeypatch):
    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(module.asyncio, "sleep", no_sleep)
    conversations = FakeConversations(commits=[None] * 6, holds=False)
    store = make_store(conversations)
    with pytest.raises(OwnershipLostError):
        await store.put(ConversationState(KEY, status="idle"))
