import random

import pytest

from silk_agent.stores.agent_platform import (
    AgentPlatformSessions,
    AgentPlatformSessionStore,
    to_platform_session_id,
)

KEY = {"project_key": "proj", "session_id": "11111111-2222-3333-4444-555555555555"}


class FakeSessionsApi:
    def __init__(self):
        self.created: list[dict] = []
        self.known: set[str] = set()

    def create(self, *, name, user_id, config=None):
        sid = config["session_id"]
        self.created.append({"name": name, "user_id": user_id, "config": config})
        if sid in self.known:
            raise RuntimeError("409 ALREADY_EXISTS: Session already exists")
        self.known.add(sid)


class FakeEventsApi:
    """Stores events per session; `list` can return them shuffled to prove ordering by seq."""

    def __init__(self, sessions: FakeSessionsApi, shuffle=False, fail_append=False):
        self.sessions, self.shuffle, self.fail_append = sessions, shuffle, fail_append
        self.events: dict[str, list[dict]] = {}
        self.calls: list[dict] = []

    def append(self, *, name, author, invocation_id, timestamp, config=None):
        if self.fail_append:
            raise RuntimeError("503 unavailable")
        self.calls.append({"name": name, "author": author, "invocation_id": invocation_id})
        self.events.setdefault(name, []).append({"raw_event": config["raw_event"]})

    def list(self, *, name, config=None):
        if name not in self.events:
            raise RuntimeError("404 NOT_FOUND")
        out = list(self.events[name])
        if self.shuffle:
            random.Random(1).shuffle(out)
        return iter(out)


def make(shuffle=False, **kw):
    sapi = FakeSessionsApi()
    eapi = FakeEventsApi(sapi, shuffle=shuffle, **kw)
    api = AgentPlatformSessions(
        "projects/p/locations/l/reasoningEngines/1", sessions_api=sapi, events_api=eapi
    )
    return AgentPlatformSessionStore(api), sapi, eapi


def e(uuid, n):
    return {"type": "user", "uuid": uuid, "n": n}


def test_session_id_mapping():
    assert to_platform_session_id("11111111-2222-3333-4444-555555555555") == (
        "c-11111111-2222-3333-4444-555555555555"
    )
    weird = to_platform_session_id("Weird_ID/with:chars" + "x" * 80)
    assert weird.startswith("c-") and len(weird) <= 63


async def test_append_then_load_roundtrip_in_order():
    store, sapi, eapi = make()
    await store.append(KEY, [e("b", 1), e("a", 2)])
    await store.append(KEY, [e("z", 3)])
    assert await store.load(KEY) == [e("b", 1), e("a", 2), e("z", 3)]
    assert sapi.created[0]["config"]["session_id"] == "c-" + KEY["session_id"]
    assert sapi.created[0]["config"]["ttl"] == "2592000s"
    assert len(sapi.created) == 1  # created once, then cached
    assert eapi.calls[0]["author"] == "claude-agent-sdk"


async def test_order_is_by_seq_not_listing_order():
    store, _, _ = make(shuffle=True)
    entries = [e(f"u{i}", i) for i in range(20)]
    await store.append(KEY, entries[:10])
    await store.append(KEY, entries[10:])
    assert await store.load(KEY) == entries


async def test_duplicate_uuid_appears_once():
    store, _, _ = make()
    await store.append(KEY, [e("a", 1), e("b", 2)])
    await store.append(KEY, [e("a", 1)])  # retried batch
    assert await store.load(KEY) == [e("a", 1), e("b", 2)]


async def test_entries_without_uuid_are_not_deduplicated():
    store, _, _ = make()
    title = {"type": "custom-title", "title": "t"}
    await store.append(KEY, [title, title])
    assert await store.load(KEY) == [title, title]


async def test_unknown_key_and_subpath_return_none():
    store, _, _ = make()
    assert await store.load({"project_key": "p", "session_id": "nope"}) is None
    await store.append(KEY, [e("x", 1)])
    assert await store.load({**KEY, "subpath": "subagents/agent-1"}) is None
    await store.append({**KEY, "subpath": "subagents/agent-1"}, [e("s", 1)])
    assert await store.load({**KEY, "subpath": "subagents/agent-1"}) == [e("s", 1)]
    assert await store.load(KEY) == [e("x", 1)]  # main transcript unaffected


async def test_empty_append_is_noop_and_existing_session_is_tolerated():
    store, sapi, _ = make()
    await store.append(KEY, [])
    assert not sapi.created
    sapi.known.add("c-" + KEY["session_id"])  # created by an earlier process
    await store.append(KEY, [e("a", 1)])
    assert await store.load(KEY) == [e("a", 1)]


async def test_append_failure_propagates_so_sdk_can_retry_and_warn():
    store, _, _ = make(fail_append=True)
    with pytest.raises(RuntimeError):
        await store.append(KEY, [e("a", 1)])
