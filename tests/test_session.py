import asyncio

from silk_agent.core.events import (
    AssistantText,
    Heartbeat,
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


async def test_stop_when_idle(clock, channel):
    env = Env(clock, channel)
    s = await env.registry.get_or_create("k")
    assert await s.stop() is False
    assert not channel.events


async def test_new_message_interrupts_by_default(clock, channel):
    env = Env(clock, channel, lambda req: [Tool("bash", duration=100), Text(req.text)])
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
