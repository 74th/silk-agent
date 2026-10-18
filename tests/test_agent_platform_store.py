import datetime as dt
import random

import pytest

from silk_agent.stores.agent_platform import (
    AgentPlatformSessions,
    AgentPlatformSessionStore,
    control_session_id,
    to_platform_session_id,
)

KEY = {"project_key": "proj", "session_id": "11111111-2222-3333-4444-555555555555"}


class FakeSessionsApi:
    def __init__(self):
        self.created: list[dict] = []
        self.known: set[str] = set()
        self.expiry: dict[str, dt.datetime] = {}
        self.gets: list[str] = []
        self.updates: list[dict] = []
        self.fail_update = False

    def create(self, *, name, user_id, config=None):
        sid = config["session_id"]
        self.created.append({"name": name, "user_id": user_id, "config": config})
        if sid in self.known:
            raise RuntimeError("409 ALREADY_EXISTS: Session already exists")
        self.known.add(sid)
        self.expiry[sid] = dt.datetime.now(dt.UTC) + dt.timedelta(days=30)

    # Agent Platform: `get` returns `expire_time`; `_update` (mask ttl) sets it to now + ttl.
    def get(self, *, name):
        sid = name.rsplit("/", 1)[-1]
        self.gets.append(sid)
        if sid not in self.known:
            raise RuntimeError("404 NOT_FOUND")
        return {"name": name, "expire_time": self.expiry.get(sid)}

    def _update(self, *, name, config):
        sid = name.rsplit("/", 1)[-1]
        self.updates.append({"sid": sid, "config": config})
        if self.fail_update:
            raise RuntimeError("503 unavailable")
        if sid not in self.known:
            raise RuntimeError("404 NOT_FOUND")
        seconds = int(config["ttl"].removesuffix("s"))
        self.expiry[sid] = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=seconds)

    def delete_session(self, sid):  # what TTL does
        self.known.discard(sid)


class FakeEventsApi:
    """Stores events per session; `list` can return them shuffled to prove ordering by seq."""

    def __init__(self, sessions: FakeSessionsApi, shuffle=False, fail_append=False):
        self.sessions, self.shuffle, self.fail_append = sessions, shuffle, fail_append
        self.events: dict[str, list[dict]] = {}
        self.calls: list[dict] = []

    def append(self, *, name, author, invocation_id, timestamp, config=None):
        if self.fail_append:
            raise RuntimeError("503 unavailable")
        if name.rsplit("/", 1)[-1] not in self.sessions.known:
            raise RuntimeError("404 NOT_FOUND: session does not exist")
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


SID = "c-" + KEY["session_id"]


def make_api(ttl_seconds=30 * 24 * 3600):
    sapi = FakeSessionsApi()
    eapi = FakeEventsApi(sapi)
    api = AgentPlatformSessions(
        "projects/p/locations/l/reasoningEngines/1",
        ttl_seconds=ttl_seconds,
        sessions_api=sapi,
        events_api=eapi,
    )
    return api, sapi, eapi


def set_remaining(api, sapi, sid, remaining):
    sapi.known.add(sid)
    sapi.expiry[sid] = dt.datetime.now(dt.UTC) + remaining
    api._expiry.pop(sid, None)


async def test_extend_is_skipped_while_more_than_half_of_the_ttl_is_left():
    api, sapi, _ = make_api()
    await api.ensure_session(SID)  # created just now: the expiry is known without a get
    assert await api.extend(SID) is False
    assert sapi.updates == [] and sapi.gets == []
    set_remaining(api, sapi, SID, dt.timedelta(days=16))
    assert await api.extend(SID) is False  # read once from `get`, still above half
    assert sapi.updates == []


async def test_extend_pushes_the_expiry_when_less_than_half_is_left():
    api, sapi, _ = make_api()
    set_remaining(api, sapi, SID, dt.timedelta(days=10))
    assert await api.extend(SID) is True
    assert sapi.updates == [
        {
            "sid": SID,
            "config": {"ttl": "2592000s", "update_mask": "ttl", "wait_for_completion": True},
        }
    ]
    assert sapi.expiry[SID] - dt.datetime.now(dt.UTC) > dt.timedelta(days=29)
    assert await api.extend(SID) is False  # just extended: nothing to do
    assert len(sapi.updates) == 1 and sapi.gets == [SID]


async def test_extend_failure_is_logged_not_raised_and_retried_next_time():
    api, sapi, _ = make_api()
    set_remaining(api, sapi, SID, dt.timedelta(days=1))
    sapi.fail_update = True
    assert await api.extend(SID) is False  # no exception
    sapi.fail_update = False
    assert await api.extend(SID) is True
    assert len(sapi.updates) == 2


async def test_extend_of_a_missing_session_is_a_noop_that_is_not_asked_twice():
    api, sapi, _ = make_api()
    assert await api.extend(SID) is False
    assert await api.extend(SID) is False
    assert sapi.gets == [SID] and sapi.updates == []
    await api.ensure_session(SID)  # created later by this process
    set_remaining(api, sapi, SID, dt.timedelta(days=1))
    api._expiry[SID] = sapi.expiry[SID]
    assert await api.extend(SID) is True


async def test_extend_for_turn_covers_the_history_and_control_sessions():
    api, sapi, _ = make_api()
    store = AgentPlatformSessionStore(api)
    control = control_session_id("slack:T:C:1.0")
    set_remaining(api, sapi, SID, dt.timedelta(days=2))
    set_remaining(api, sapi, control, dt.timedelta(days=3))
    await store.extend_for_turn("slack:T:C:1.0", KEY["session_id"])
    assert {u["sid"] for u in sapi.updates} == {SID, control}
    await store.extend_for_turn("slack:T:C:1.0", None)  # a first turn has no history yet


async def test_session_exists_is_false_only_for_a_definite_not_found():
    api, sapi, _ = make_api()
    store = AgentPlatformSessionStore(api)
    assert await store.session_exists(KEY["session_id"]) is False
    sapi.known.add(SID)
    assert await store.session_exists(KEY["session_id"]) is True

    def boom(*, name):
        raise RuntimeError("503 unavailable")

    api2, sapi2, _ = make_api()
    sapi2.get = boom
    assert await AgentPlatformSessionStore(api2).session_exists(KEY["session_id"]) is True


async def test_append_to_an_expired_session_recreates_it_and_notes_the_loss():
    store, sapi, eapi = make()
    await store.append(KEY, [e("a", 1)])
    assert not store.take_history_loss(KEY["session_id"])
    sapi.delete_session(SID)  # TTL removed it while this process still had it cached
    eapi.events.pop(f"projects/p/locations/l/reasoningEngines/1/sessions/{SID}")
    await store.append(KEY, [e("b", 2)])
    assert len(sapi.created) == 2  # created again, user id kept
    assert sapi.created[1]["user_id"] == sapi.created[0]["user_id"]
    assert await store.load(KEY) == [e("b", 2)]  # earlier history is gone
    assert store.take_history_loss(KEY["session_id"]) is True
    assert store.take_history_loss(KEY["session_id"]) is False  # taken once


async def test_append_retry_happens_once():
    api, sapi, eapi = make_api()
    await api.ensure_session(SID)
    sapi.delete_session(SID)

    async def always_gone(session_id, user_id="silk-agent", state=None):
        return None

    api.ensure_session = always_gone  # recreate "succeeds" but the session is still missing
    with pytest.raises(RuntimeError, match="404"):
        await api.append_event(SID, {"x": 1})
