import asyncio

from silk_agent.core.events import TurnFinished, TurnInterrupted, UserMessage
from silk_agent.core.lifecycle import new_bot_instance_id, recover_interrupted
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.session import SessionSettings
from silk_agent.core.stopwords import StopWords
from silk_agent.testing.fakes import (
    AllowAllGuard,
    FakeAgentEngine,
    InMemoryStateStore,
    NoopSanitizer,
    ScriptedChannel,
    Text,
    Tool,
)


def make_bot(clock, channel, store, scenario, bot_id):
    engines = {}

    def factory(key):
        engines[key] = FakeAgentEngine(clock, scenario)
        return engines[key]

    orch = ConversationOrchestrator(
        factory,
        channel,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(["停止"]),
        settings=SessionSettings(),
        state_store=store,
        bot_instance_id=bot_id,
    )
    return orch, engines


def test_instance_ids_are_unique():
    assert new_bot_instance_id() != new_bot_instance_id()


async def test_crash_is_detected_on_next_start_and_not_resumed(clock):
    store = InMemoryStateStore()
    ch1 = ScriptedChannel()
    bot1, _ = make_bot(clock, ch1, store, [Tool("bash", duration=600)], "bot-1")
    await bot1.handle(UserMessage("k", "u", "long job"))
    await clock.advance(1)
    assert (await store.get("k")).status == "running"
    # --- the process dies here: bot1 is simply abandoned (no shutdown, no state change) ---

    ch2 = ScriptedChannel()
    bot2, engines2 = make_bot(clock, ch2, store, [Text("x")], "bot-2")
    recovered = await recover_interrupted(store, ch2, "bot-2", clock)
    assert recovered == ["k"]
    interrupted = ch2.of(TurnInterrupted)
    assert len(interrupted) == 1 and interrupted[0].reason == "crash"
    assert interrupted[0].conversation_key == "k"
    assert (await store.get("k")).status == "interrupted_crash"
    # nothing restarts by itself
    await clock.advance(120)
    assert not engines2 and not ch2.of(TurnFinished)
    # and a second start-up does not report it again
    assert await recover_interrupted(store, ch2, "bot-3", clock) == []


async def test_own_running_turns_are_not_treated_as_crashed(clock):
    store = InMemoryStateStore()
    ch = ScriptedChannel()
    bot, _ = make_bot(clock, ch, store, [Tool("bash", duration=600)], "bot-1")
    await bot.handle(UserMessage("k", "u", "job"))
    await clock.advance(1)
    assert await recover_interrupted(store, ch, "bot-1", clock) == []


async def test_shutdown_interrupts_long_turn_after_grace(clock):
    store = InMemoryStateStore()
    ch = ScriptedChannel()
    bot, engines = make_bot(clock, ch, store, [Tool("bash", duration=600)], "bot-1")
    await bot.handle(UserMessage("k", "u", "job"))
    await clock.advance(1)
    task = asyncio.create_task(bot.shutdown(25))
    await clock.advance(24)
    assert not task.done()
    await clock.advance(2)
    await asyncio.wait_for(task, 2)
    interrupted = ch.of(TurnInterrupted)
    assert [i.reason for i in interrupted] == ["shutdown"]
    assert (await store.get("k")).status == "interrupted_shutdown"
    assert engines["k"].closed
    # a later start-up does not report it as a crash: it was already recorded
    assert await recover_interrupted(store, ScriptedChannel(), "bot-2", clock) == []


async def test_shutdown_lets_short_turn_finish(clock):
    store = InMemoryStateStore()
    ch = ScriptedChannel()
    bot, _ = make_bot(clock, ch, store, [Tool("bash", duration=10), Text("done")], "bot-1")
    await bot.handle(UserMessage("k", "u", "job"))
    await clock.advance(1)
    task = asyncio.create_task(bot.shutdown(25))
    await clock.advance(10)
    await asyncio.wait_for(task, 2)
    assert ch.of(TurnFinished) and not ch.of(TurnInterrupted)
    assert (await store.get("k")).status == "idle"


async def test_resume_after_crash_continues_session_and_tells_agent(clock):
    store = InMemoryStateStore()
    ch1 = ScriptedChannel()
    bot1, e1 = make_bot(clock, ch1, store, [Tool("bash", duration=600)], "bot-1")
    await bot1.handle(UserMessage("k", "u", "long job"))
    await clock.advance(1)
    st = await store.get("k")
    st.agent_session_id = "claude-sess-1"
    st.running_command_ids = ["cmd-7"]
    await store.put(st)
    # crash, restart
    ch2 = ScriptedChannel()
    bot2, e2 = make_bot(clock, ch2, store, [Text("続けます")], "bot-2")
    await recover_interrupted(store, ch2, "bot-2", clock)
    await bot2.handle(UserMessage("k", "u", "続けて"))
    await ch2.wait_for(TurnFinished)
    request = e2["k"].requests[0]
    assert request.resume_session_id == "claude-sess-1"
    assert request.text == "続けて"
    assert len(request.notices) == 1
    assert "中断" in request.notices[0] and "cmd-7" in request.notices[0]
    assert "再実行しない" in request.notices[0]
    state = await store.get("k")
    assert state.status == "idle" and state.running_command_ids == []
    # the next turn carries no resume notice any more
    await bot2.handle(UserMessage("k", "u", "もう一度"))
    await ch2.wait_for(TurnFinished, count=2)
    assert e2["k"].requests[1].notices == ()


async def test_interrupted_conversation_does_not_start_without_user_input(clock):
    store = InMemoryStateStore()
    ch = ScriptedChannel()
    bot, engines = make_bot(clock, ch, store, [Tool("bash", duration=600)], "bot-1")
    await bot.handle(UserMessage("k", "u", "job"))
    await clock.advance(1)
    ch2 = ScriptedChannel()
    bot2, engines2 = make_bot(clock, ch2, store, [Text("x")], "bot-2")
    await recover_interrupted(store, ch2, "bot-2", clock)
    await clock.advance(600)
    assert not engines2 and not ch2.of(TurnFinished)


async def test_agent_session_id_is_saved_from_engine(clock):
    store = InMemoryStateStore()
    ch = ScriptedChannel()
    bot, engines = make_bot(clock, ch, store, [Text("x")], "bot-1")

    await bot.handle(UserMessage("k", "u", "hi"))
    await ch.wait_for(TurnFinished)
    engines["k"].session_id = "abc"  # what ClaudeAgentEngine exposes after its first result
    await bot.handle(UserMessage("k", "u", "again"))
    await ch.wait_for(TurnFinished, count=2)
    assert (await store.get("k")).agent_session_id == "abc"
