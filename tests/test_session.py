import asyncio

import pytest

from silk_agent.core.events import (
    AssistantText,
    Heartbeat,
    MessageAppended,
    Notice,
    StatusChanged,
    ToolFinished,
    TurnFinished,
    TurnInterrupted,
    TurnStarted,
    UserMessage,
)
from silk_agent.core.ports import ConversationState
from silk_agent.core.session import AgentSessionRegistry, SessionSettings
from silk_agent.testing.fakes import (
    FakeAgentEngine,
    InMemoryStateStore,
    Text,
    Tool,
    Wait,
)


class Env:
    """Registry wired to fakes; one FakeAgentEngine per conversation."""

    def __init__(self, clock, channel, scenarios=None, **settings):
        self.engines: dict[str, FakeAgentEngine] = {}
        self.factory_calls = 0
        self.cancelled: list[str] = []
        self.store = InMemoryStateStore()

        def factory(key):
            self.factory_calls += 1
            eng = FakeAgentEngine(clock, scenarios)
            self.engines[key] = eng
            return eng

        async def cancel(key):
            self.cancelled.append(key)

        self.registry = AgentSessionRegistry(
            factory,
            channel.send,
            clock,
            SessionSettings(**settings),
            state_store=self.store,
            bot_instance_id="bot-1",
            cancel_commands=cancel,
        )

    async def send(self, key, text):
        s = await self.registry.get_or_create(key)
        await s.submit(UserMessage(key, "u", text))
        return s


# ---------------------------------------------------------------- 2.3


async def test_message_during_turn_does_not_start_new_instance(clock, channel):
    env = Env(clock, channel, [Tool("bash", "ls", duration=30), Text("done")])
    await env.send("k", "first")
    await channel.wait_for(TurnStarted)
    await env.send("k", "second")  # while running
    await clock.advance(31)
    await channel.wait_for(TurnFinished)
    assert env.factory_calls == 1
    assert len(env.registry.sessions()) == 1


async def test_concurrent_first_messages_create_one_session(clock, channel):
    env = Env(clock, channel, [Text("hi")])
    sessions = await asyncio.gather(*(env.registry.get_or_create("k") for _ in range(10)))
    assert len({id(s) for s in sessions}) == 1
    await asyncio.gather(env.send("k", "a"), env.send("k", "b"))
    await channel.wait_for(TurnFinished, count=1)
    await clock.advance(1)
    assert env.factory_calls == 1


async def test_same_conversation_is_processed_in_order(clock, channel):
    env = Env(clock, channel, lambda req: [Text(req.text)], interrupt_mode="queue")
    await env.send("k", "A")
    await env.send("k", "B")
    await channel.wait_for(TurnFinished, count=2)
    texts = [e.text for e in channel.of(AssistantText)]
    assert texts == ["A", "B"]
    assert [r.text for r in env.engines["k"].requests] == ["A", "B"]


async def test_conversations_are_independent(clock, channel):
    env = Env(clock, channel, lambda req: [Text(req.conversation_key)])
    await env.send("k1", "x")
    await env.send("k2", "y")
    await channel.wait_for(TurnFinished, count=2)
    for k in ("k1", "k2"):
        assert [e.text for e in channel.of(AssistantText, k)] == [k]
    assert env.factory_calls == 2


async def test_turn_event_order(clock, channel):
    env = Env(clock, channel, [Text("hello")])
    await env.send("k", "hi")
    await channel.wait_for(TurnFinished)
    assert channel.kinds("k") == ["TurnStarted", "AssistantText", "TurnFinished"]
    state = await env.store.get("k")
    assert state is not None and state.status == "idle" and state.bot_instance_id == "bot-1"


async def test_input_start_and_completion_callbacks_follow_queued_turns(clock, channel):
    started = []
    completed = []

    async def mark_started(input_id):
        started.append(input_id)
        return True

    async def mark_completed(input_id):
        completed.append(input_id)

    registry = AgentSessionRegistry(
        lambda _key: FakeAgentEngine(clock, lambda request: [Text(request.text)]),
        channel.send,
        clock,
        SessionSettings(interrupt_mode="queue"),
        input_started=mark_started,
        input_completed=mark_completed,
    )
    session = await registry.get_or_create("k")
    assert await session.submit(UserMessage("k", "u", "first", input_id="input-1")) == "queued"
    assert await session.submit(UserMessage("k", "u", "second", input_id="input-2")) == "queued"
    await channel.wait_for(TurnFinished, count=2)
    assert started == ["input-1", "input-2"]
    assert completed == ["input-1", "input-2"]


# ---------------------------------------------------------------- 2.4


async def test_stop_while_running_tool(clock, channel):
    env = Env(clock, channel, [Tool("bash", "sleep", duration=300), Text("never")])
    s = await env.send("k", "go")
    await channel.wait_for(TurnStarted)
    await clock.advance(5)
    assert await s.stop() is True
    assert env.cancelled == ["k"]
    assert channel.of(ToolFinished)[0].status == "interrupted"
    assert channel.of(TurnInterrupted)[0].reason == "user"
    assert [e.status for e in channel.of(StatusChanged)] == ["interrupting", "interrupted"]
    assert s.state == "idle"
    # next input is accepted
    env.engines["k"]._scenarios = [Text("again")]
    await s.submit(UserMessage("k", "u", "again"))
    await channel.wait_for(TurnFinished)
    assert channel.of(AssistantText)[-1].text == "again"


async def test_shutdown_drain_and_lease_loss_do_not_cancel_remote_commands(clock, channel):
    for reason in ("shutdown", "drain", "lease_lost"):
        env = Env(clock, channel, [Tool("bash", "sleep", duration=300), Text("never")])
        starts = len(channel.of(TurnStarted))
        session = await env.send(f"k-{reason}", "work")
        await channel.wait_for(TurnStarted, count=starts + 1)
        if reason == "lease_lost":
            await session.aclose(reason="lease_lost")
        else:
            await session.shutdown(0, reason=reason)
        assert env.cancelled == []


async def test_shutdown_state_save_failure_is_reported_to_the_worker(clock, channel):
    env = Env(clock, channel, [Tool("bash", "sleep", duration=300), Text("never")])

    async def fail_put(_state):
        raise OSError("state store unavailable")

    env.store.put = fail_put
    session = await env.send("save-failure", "work")
    await channel.wait_for(TurnStarted)

    with pytest.raises(RuntimeError, match="ownership must remain held"):
        await session.shutdown(0, reason="drain")

    assert env.cancelled == []
    assert session.engine is None


async def test_shutdown_without_deadline_waits_for_the_current_turn(clock, channel):
    env = Env(clock, channel, [Tool("bash", "sleep", duration=300), Text("done")])
    session = await env.send("unbounded-shutdown", "work")
    await channel.wait_for(TurnStarted)

    closing = asyncio.create_task(session.shutdown())
    await asyncio.sleep(0)
    assert not closing.done()
    assert env.cancelled == []

    await clock.advance(300)
    await channel.wait_for(TurnFinished)
    await closing
    assert session.state == "idle"


async def test_shutdown_deadline_interrupts_turn_and_persists_shutdown_state(clock, channel):
    env = Env(clock, channel, [Tool("bash", "sleep", duration=300), Text("never")])
    session = await env.send("bounded-shutdown", "work")
    await channel.wait_for(TurnStarted)

    closing = asyncio.create_task(session.shutdown(5, reason="drain"))
    await asyncio.sleep(0)
    assert not closing.done()
    await clock.advance(5)
    await closing

    state = await env.store.get("bounded-shutdown")
    assert state is not None and state.status == "interrupted_shutdown"
    assert env.engines["bounded-shutdown"].interrupt_count == 1
    assert env.cancelled == []


async def test_stop_when_idle(clock, channel):
    env = Env(clock, channel)
    s = await env.registry.get_or_create("k")
    assert await s.stop() is False
    assert not channel.events


async def test_new_message_is_appended_to_the_running_turn_by_default(clock, channel):
    env = Env(clock, channel, lambda req: [Tool("bash", duration=100), Text(req.text)])
    s = await env.send("k", "T1")
    await channel.wait_for(TurnStarted)
    await s.submit(UserMessage("k", "u", "特に止まってない？"))
    # Not interrupted, no second engine, no new turn: the running turn gets the message.
    assert env.engines["k"].appended == ["特に止まってない？"]
    assert len(channel.of(MessageAppended)) == 1  # shown in the progress message, no post
    assert env.engines["k"].interrupt_count == 0
    await clock.advance(100)
    await channel.wait_for(TurnFinished)
    assert not channel.of(TurnInterrupted)
    assert [r.text for r in env.engines["k"].requests] == ["T1"]
    assert env.factory_calls == 1 and env.cancelled == []


async def test_completion_notification_appends_to_running_turn_and_tracks_input(clock, channel):
    env = Env(clock, channel, lambda req: [Tool("bash", duration=30), Text(req.text)])
    completed = []

    async def mark_completed(input_id):
        completed.append(input_id)

    registry = AgentSessionRegistry(
        env.registry._engine_factory,
        channel.send,
        clock,
        input_completed=mark_completed,
    )
    session = await registry.get_or_create("k")
    await session.submit(UserMessage("k", "u", "work"))
    await channel.wait_for(TurnStarted)
    assert await session.notify("command finished", input_id="finished-1") == "appended"
    assert env.engines["k"].appended == ["command finished"]
    await clock.advance(30)
    await channel.wait_for(TurnFinished)
    assert completed == ["finished-1"]


async def test_completion_notification_starts_idle_turn(clock, channel):
    env = Env(clock, channel, lambda request: [Text(request.text)])
    session = await env.registry.get_or_create("k")
    assert await session.notify("command finished", input_id="finished-idle") == "queued"
    await channel.wait_for(TurnFinished)
    assert env.engines["k"].requests[0].text == "command finished"


async def test_completion_notification_is_held_after_interruption(clock, channel):
    env = Env(clock, channel, [Tool("bash", "sleep", duration=300), Text("never")])
    session = await env.send("k", "work")
    await channel.wait_for(TurnStarted)
    assert await session.stop()
    assert await session.notify("command finished", input_id="finished-held") == "held"
    assert channel.of(Notice)[-1].text == "バックグラウンド処理が完了しました。結果は保留中です。"


async def test_a_held_notice_reaches_the_next_turn_in_single_pod_mode(clock, channel):
    env = Env(
        clock,
        channel,
        lambda req: (
            [Tool("bash", duration=300), Text("never")] if req.text == "work" else [Text("ok")]
        ),
    )
    session = await env.send("k", "work")
    await channel.wait_for(TurnStarted)
    assert await session.stop()
    assert await session.notify("make build: exit code 0") == "held"
    await session.submit(UserMessage("k", "u", "どうなった？"))
    await channel.wait_for(TurnFinished)
    assert env.engines["k"].requests[-1].notices == ("make build: exit code 0",)
    # Delivered once.
    await session.submit(UserMessage("k", "u", "もう一度"))
    await channel.wait_for(TurnFinished, count=2)
    assert env.engines["k"].requests[-1].notices == ()


async def test_a_durable_held_input_is_not_also_kept_in_memory(clock, channel):
    env = Env(clock, channel, [Tool("bash", "sleep", duration=300), Text("never")])
    session = await env.send("k", "work")
    await channel.wait_for(TurnStarted)
    assert await session.stop()
    assert await session.notify("done", input_id="finished-1") == "held"
    assert session._held_notices == []  # the inbox holds and releases it


async def test_interrupt_mode_does_not_hold_notices_for_the_replacing_turn(clock, channel):
    env = Env(
        clock,
        channel,
        lambda req: [Tool("bash", duration=100), Text(req.text)],
        interrupt_mode="interrupt",
    )
    s = await env.send("k", "T1")
    await channel.wait_for(TurnStarted)
    await s.submit(UserMessage("k", "u", "M"))  # interrupts T1, M runs next
    await channel.wait_for(TurnInterrupted)
    await channel.wait_for(TurnStarted, count=2)
    assert await s.notify("command finished") == "appended"  # M is running: join it
    assert env.engines["k"].appended == ["command finished"]


async def test_append_falls_back_to_a_queued_turn_when_the_turn_cannot_take_it(clock, channel):
    env = Env(clock, channel, lambda req: [Tool("bash", duration=10), Text(req.text)])
    s = await env.send("k", "T1")
    await channel.wait_for(TurnStarted)

    async def refuse(text):
        return False

    env.engines["k"].append = refuse  # e.g. the ResultMessage was already received
    await s.submit(UserMessage("k", "u", "M"))
    await clock.advance(10)
    await channel.wait_for(TurnFinished, count=1)
    await clock.advance(10)
    await channel.wait_for(TurnFinished, count=2)
    assert [r.text for r in env.engines["k"].requests] == ["T1", "M"]
    assert not channel.of(TurnInterrupted)


async def test_interrupt_mode_stops_the_turn_for_a_new_message(clock, channel):
    env = Env(
        clock,
        channel,
        lambda req: [Tool("bash", duration=100), Text(req.text)],
        interrupt_mode="interrupt",
    )
    s = await env.send("k", "T1")
    await channel.wait_for(TurnStarted)
    env.engines["k"]._scenarios = lambda req: [Text("answer:" + req.text)]
    await s.submit(UserMessage("k", "u", "M"))
    await channel.wait_for(TurnFinished)
    interrupted = channel.of(TurnInterrupted)
    assert [i.reason for i in interrupted] == ["new_message"]
    assert [r.text for r in env.engines["k"].requests] == ["T1", "M"]
    assert channel.of(AssistantText)[-1].text == "answer:M"
    assert env.factory_calls == 1  # T2 continues on the same engine = same history
    assert env.cancelled == []  # sandbox commands are not killed by a new message


async def test_queue_mode_waits_for_turn(clock, channel):
    env = Env(
        clock,
        channel,
        lambda req: [Tool("bash", duration=10), Text(req.text)],
        interrupt_mode="queue",
    )
    await env.send("k", "T1")
    await channel.wait_for(TurnStarted)
    await env.send("k", "M")
    await clock.advance(10)
    await channel.wait_for(TurnFinished, count=1)
    await clock.advance(10)
    await channel.wait_for(TurnFinished, count=2)
    assert not channel.of(TurnInterrupted)
    assert [e.text for e in channel.of(AssistantText)] == ["T1", "M"]


async def test_stop_drops_queued_messages(clock, channel):
    env = Env(clock, channel, [Tool("bash", duration=100)], interrupt_mode="queue")
    s = await env.send("k", "T1")
    await channel.wait_for(TurnStarted)
    await env.send("k", "queued")
    await s.stop()
    await asyncio.sleep(0.01)
    assert [r.text for r in env.engines["k"].requests] == ["T1"]


async def test_reset_starts_fresh_session(clock, channel):
    env = Env(clock, channel, [Text("ok")])
    s = await env.send("k", "remember 42")
    await channel.wait_for(TurnFinished)
    await env.store.put(ConversationState("k", agent_session_id="abc"))
    await s.reset()
    assert env.engines["k"].reset_count == 1
    state = await env.store.get("k")
    assert state is not None and state.agent_session_id is None


async def test_idle_engine_is_closed_and_recreated(clock, channel):
    env = Env(clock, channel, [Text("ok")], idle_timeout_seconds=600)
    await env.send("k", "a")
    await channel.wait_for(TurnFinished)
    first = env.engines["k"]
    await clock.advance(601)
    assert first.closed
    await env.send("k", "b")
    await channel.wait_for(TurnFinished, count=2)
    assert env.factory_calls == 2


# ---------------------------------------------------------------- 2.5


async def test_heartbeat_during_long_turn(clock, channel):
    env = Env(clock, channel, [Wait(150), Text("done")], heartbeat_interval_seconds=60)
    await env.send("k", "go")
    await channel.wait_for(TurnStarted)
    await clock.advance(150)
    await channel.wait_for(TurnFinished)
    assert len(channel.of(Heartbeat)) == 2
    await clock.advance(300)
    assert len(channel.of(Heartbeat)) == 2


async def test_heartbeat_three_over_three_minutes(clock, channel):
    env = Env(clock, channel, [Wait(200)], heartbeat_interval_seconds=60)
    await env.send("k", "go")
    await channel.wait_for(TurnStarted)
    await clock.advance(180)
    assert len(channel.of(Heartbeat)) == 3


# ---------------------------------------------------------------- sandbox start-up


async def _turn_with_sandbox(clock, channel, ready: bool):
    gate = asyncio.Event()
    order: list[str] = []

    async def notices(key):
        order.append("ensure")
        await gate.wait()  # creating / resuming the sandbox
        return []

    async def is_ready(key):
        return ready

    registry = AgentSessionRegistry(
        lambda key: FakeAgentEngine(clock, [Text("ok")]),
        channel.send,
        clock,
        SessionSettings(),
        notice_source=notices,
        sandbox_ready=is_ready,
    )
    s = await registry.get_or_create("k")
    await s.submit(UserMessage("k", "u", "hi"))
    await channel.wait_for(TurnStarted)  # shown before the sandbox is ready
    await asyncio.sleep(0.01)
    assert order == ["ensure"] and not channel.of(TurnFinished)
    gate.set()
    await channel.wait_for(TurnFinished)
    return [e.status for e in channel.of(StatusChanged)]


async def test_turn_shows_sandbox_start_up_when_not_running(clock, channel):
    statuses = await _turn_with_sandbox(clock, channel, ready=False)
    assert statuses == ["preparing_sandbox", "thinking"]


async def test_running_sandbox_shows_nothing_extra(clock, channel):
    assert await _turn_with_sandbox(clock, channel, ready=True) == []


async def test_stop_while_sandbox_is_preparing_skips_engine_query(clock, channel):
    ensure_entered = asyncio.Event()
    ensure_release = asyncio.Event()
    engine = FakeAgentEngine(clock, [Tool("bash", duration=60)])

    async def ensure(_key):
        ensure_entered.set()
        await ensure_release.wait()
        return []

    async def not_ready(_key):
        return False

    registry = AgentSessionRegistry(
        lambda _key: engine,
        channel.send,
        clock,
        SessionSettings(),
        notice_source=ensure,
        sandbox_ready=not_ready,
    )
    session = await registry.get_or_create("k")
    try:
        await session.submit(UserMessage("k", "u", "hello"))
        await channel.wait_for(TurnStarted)
        await asyncio.wait_for(ensure_entered.wait(), timeout=2)
        assert await session.request_stop(reason="user")
        ensure_release.set()
        await channel.wait_for(TurnInterrupted)
        assert engine.requests == []
        assert channel.of(TurnFinished) == []
    finally:
        ensure_release.set()
        await registry.aclose()


class _Released:
    def __init__(self):
        self.keys = []

    def __call__(self, key):
        self.keys.append(key)


def _release_env(clock, channel, **kw):
    released = _Released()
    store = InMemoryStateStore()

    def factory(key):
        return FakeAgentEngine(clock, lambda req: [Text("ok")])

    registry = AgentSessionRegistry(
        factory,
        channel.send,
        clock,
        SessionSettings(idle_timeout_seconds=300),
        state_store=store,
        release_idle_sessions=kw.get("release", True),
        on_session_released=released,
    )
    return registry, released


async def test_an_idle_session_is_released_and_the_next_input_gets_a_fresh_one(clock, channel):
    registry, released = _release_env(clock, channel)
    first = await registry.get_or_create("k")
    await first.submit(UserMessage("k", "u", "hello"))
    await channel.wait_for(TurnFinished)
    await clock.advance(301)  # past the idle timeout
    for _ in range(20):
        await asyncio.sleep(0)
    assert registry.get("k") is None and released.keys == ["k"]
    second = await registry.get_or_create("k")
    assert second is not first
    await second.submit(UserMessage("k", "u", "again"))
    await channel.wait_for(TurnFinished, count=2)


async def test_a_session_is_kept_while_it_holds_something(clock, channel):
    registry, released = _release_env(clock, channel)
    session = await registry.get_or_create("k")
    session._held_notices.append("a result nobody has seen")  # waits for the next turn
    await session.submit(UserMessage("k", "u", "hello"))
    await channel.wait_for(TurnFinished)
    session._held_notices.append("held again")
    await clock.advance(301)
    for _ in range(20):
        await asyncio.sleep(0)
    assert registry.get("k") is session and released.keys == []


async def test_sessions_are_never_released_unless_the_registry_opts_in(clock, channel):
    registry, released = _release_env(clock, channel, release=False)
    session = await registry.get_or_create("k")
    await session.submit(UserMessage("k", "u", "hello"))
    await channel.wait_for(TurnFinished)
    await clock.advance(301)
    for _ in range(20):
        await asyncio.sleep(0)
    assert registry.get("k") is session and released.keys == []
