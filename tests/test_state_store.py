from datetime import UTC, datetime

from silk_agent.core.ports import ConversationState
from silk_agent.stores.agent_platform import AgentPlatformSessions
from silk_agent.stores.state import AgentPlatformStateStore
from tests.test_agent_platform_store import FakeEventsApi, FakeSessionsApi


class ListingSessionsApi(FakeSessionsApi):
    def __init__(self):
        super().__init__()
        self.users: dict[str, str] = {}

    def create(self, *, name, user_id, config=None):
        super().create(name=name, user_id=user_id, config=config)
        self.users[config["session_id"]] = user_id

    def list(self, *, name, config=None):
        return [
            {"name": f"{name}/sessions/{sid}"}
            for sid, user in self.users.items()
            if f'user_id="{user}"' == config["filter"]
        ]


def make():
    sapi = ListingSessionsApi()
    eapi = FakeEventsApi(sapi, shuffle=True)
    api = AgentPlatformSessions(
        "projects/p/locations/l/reasoningEngines/1", sessions_api=sapi, events_api=eapi
    )
    return AgentPlatformStateStore(api), api, sapi, eapi


def state(key, status, **kw):
    return ConversationState(
        key, status=status, updated_at=datetime(2026, 10, 10, 9, 0, tzinfo=UTC), **kw
    )


async def test_put_get_roundtrip_with_progress_ref():
    store, *_ = make()
    s = state(
        "slack:T:C:1",
        "running",
        turn_id="t1",
        bot_instance_id="b1",
        progress_ref={"channel": "C", "ts": "1.2"},
        running_command_ids=["cmd-1"],
    )
    await store.put(s)
    got = await store.get("slack:T:C:1")
    assert got == s


async def test_latest_put_wins_even_after_restart():
    store, api, *_ = make()
    await store.put(state("k", "running", turn_id="t1"))
    await store.put(state("k", "idle", turn_id="t1"))
    # a new process (empty cache) reads the log: latest seq wins regardless of listing order
    fresh = AgentPlatformStateStore(api)
    assert (await fresh.get("k")).status == "idle"
    assert await fresh.get("unknown") is None


async def test_list_running_only_returns_running_conversations():
    store, api, *_ = make()
    await store.put(state("a", "running", bot_instance_id="old"))
    await store.put(state("b", "idle"))
    await store.put(state("c", "running"))
    await store.put(state("c", "interrupted_crash"))
    fresh = AgentPlatformStateStore(api)
    running = await fresh.list_running()
    assert [s.conversation_key for s in running] == ["a"]
    assert running[0].bot_instance_id == "old"
