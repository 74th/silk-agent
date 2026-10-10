import asyncio

from silk_agent.core.events import AssistantText, ToolFinished, ToolStarted, TurnFinished
from silk_agent.core.ports import ConversationState, TurnRequest
from silk_agent.testing.fakes import (
    FakeAgentEngine,
    FakeClock,
    FakeSanitizer,
    InMemoryRuntime,
    InMemoryStateStore,
    ScriptedChannel,
    Text,
    Tool,
)


async def _collect(engine, req):
    return [e async for e in engine.run_turn(req)]


async def test_engine_plays_scenario():
    clock = FakeClock()
    engine = FakeAgentEngine(clock, [Tool("bash", "ls", result="a"), Text("done")])
    events = await _collect(engine, TurnRequest("k", "t1", "hi"))
    assert [type(e) for e in events] == [ToolStarted, ToolFinished, AssistantText, TurnFinished]
    assert events[1].status == "success"


async def test_engine_failure_tool_and_interrupt():
    clock = FakeClock()
    engine = FakeAgentEngine(clock, [Tool("bash", "sleep", duration=100), Text("never")])
    task = asyncio.create_task(_collect(engine, TurnRequest("k", "t1", "hi")))
    await clock.advance(10)
    await engine.interrupt()
    events = await task
    assert [type(e) for e in events] == [ToolStarted, ToolFinished]
    assert events[1].status == "interrupted"
    assert events[1].duration_seconds == 10


async def test_fake_clock_advance_wakes_in_order():
    clock = FakeClock()
    hits = []

    async def ticker():
        while True:
            await clock.sleep(60)
            hits.append(clock.now())

    t = asyncio.create_task(ticker())
    await clock.advance(150)
    assert len(hits) == 2
    t.cancel()


async def test_channel_runtime_store_sanitizer():
    ch = ScriptedChannel()
    await ch.send(AssistantText("k", "t", "x"))
    assert ch.kinds() == ["AssistantText"]
    await ch.wait_for(AssistantText)

    rt = InMemoryRuntime()
    await rt.write_file("k1", "/a.txt", b"x")
    assert await rt.read_file("k1", "/a.txt") == b"x"
    try:
        await rt.read_file("k2", "/a.txt")
        raise AssertionError("should not see k1 files")
    except FileNotFoundError:
        pass

    store = InMemoryStateStore()
    await store.put(ConversationState("k", status="running"))
    assert [s.conversation_key for s in await store.list_running()] == ["k"]

    s = FakeSanitizer(mask_words=["a@b.c"])
    r = await s.sanitize("mail a@b.c")
    assert r.status == "masked" and r.text == "mail [MASKED]"
