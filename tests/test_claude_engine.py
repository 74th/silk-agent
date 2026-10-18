import asyncio
from pathlib import Path

from claude_agent_sdk import (
    AssistantMessage,
    MirrorErrorMessage,
    ResultMessage,
    TextBlock,
    UserMessage,
)

from silk_agent.agent.engine import (
    HISTORY_LOST_NOTICE,
    HOST_TOOLS,
    ClaudeAgentEngine,
    ClaudeEngineSettings,
)
from silk_agent.agent.summaries import display_name, summarize
from silk_agent.core.events import (
    AssistantText,
    Notice,
    ToolFinished,
    ToolStarted,
    TurnFailed,
    TurnFinished,
)
from silk_agent.core.ports import TurnRequest


def result(**kw):
    base = dict(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="sid-1",
        total_cost_usd=0.01,
        usage={"input_tokens": 10, "output_tokens": 5},
    )
    base.update(kw)
    return ResultMessage(**base)


def text(t):
    return AssistantMessage(content=[TextBlock(t)], model="m")


class FakeSDKClient:
    """Stands in for ClaudeSDKClient. `script` is a list of steps run by receive_response."""

    instances: list["FakeSDKClient"] = []

    def __init__(self, options):
        self.options = options
        self.queries: list[str] = []
        self.connected = False
        self.disconnected = False
        self.interrupted = asyncio.Event()
        self.script: list = []
        FakeSDKClient.instances.append(self)

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.disconnected = True

    async def query(self, prompt):
        self.queries.append(prompt)

    async def interrupt(self):
        self.interrupted.set()

    async def hook(self, event, **inp):
        cb = self.options.hooks[event][0].hooks[0]
        await cb({"hook_event_name": event, **inp}, inp.get("tool_use_id"), None)

    async def receive_messages(self):
        """Like the CLI with --replay-user-messages: each prompt is echoed when consumed."""
        echoed = 0
        async for message in self.receive_response():
            while echoed < len(self.queries):
                echoed += 1
                yield UserMessage(content=self.queries[echoed - 1])
            yield message

    async def receive_response(self):
        for step in self.script:
            kind = step[0]
            if kind == "msg":
                yield step[1]
            elif kind == "pre":
                await self.hook(
                    "PreToolUse", tool_name=step[1], tool_input=step[2], tool_use_id=step[3]
                )
            elif kind == "post":
                await self.hook(
                    "PostToolUse",
                    tool_name=step[1],
                    tool_input={},
                    tool_response="",
                    tool_use_id=step[2],
                )
            elif kind == "fail":
                await self.hook(
                    "PostToolUseFailure",
                    tool_name=step[1],
                    tool_input={},
                    error="x",
                    tool_use_id=step[2],
                    is_interrupt=step[3],
                )
            elif kind == "block":
                await self.interrupted.wait()
                self.interrupted.clear()
                yield result(terminal_reason="aborted_tools", result="")
                return
            elif kind == "raise":
                raise step[1]


def make(clock, tmp_path, script_for=None, **kw):
    FakeSDKClient.instances = []
    settings = ClaudeEngineSettings(
        model="claude-haiku-5-5", project="nnyn-dev", region="global", work_root=tmp_path, **kw
    )

    def factory(options):
        c = FakeSDKClient(options)
        if script_for:
            c.script = script_for(len(FakeSDKClient.instances))
        return c

    return ClaudeAgentEngine("conv", settings, clock, client_factory=factory)


async def run_resume(engine, session_id):
    request = TurnRequest("conv", "t0", "hi", (), session_id)
    return [e async for e in engine.run_turn(request)]


async def run(engine, text_="hi", turn="t1", notices=()):
    return [e async for e in engine.run_turn(TurnRequest("conv", turn, text_, notices))]


async def test_text_and_result(clock, tmp_path):
    eng = make(clock, tmp_path, lambda n: [("msg", text("こんにちは")), ("msg", result())])
    events = await run(eng)
    assert [type(e) for e in events] == [AssistantText, TurnFinished]
    assert events[0].text == "こんにちは"
    assert events[1].usage.input_tokens == 10 and events[1].usage.cost_usd == 0.01


async def test_tool_hooks_become_events(clock, tmp_path):
    script = [
        ("pre", "mcp__sandbox__bash", {"command": "uv run pytest\nsecond"}, "tu1"),
        ("post", "mcp__sandbox__bash", "tu1"),
        ("pre", "mcp__docs__search", {"query": "silk"}, "tu2"),
        ("fail", "mcp__docs__search", "tu2", False),
        ("msg", result()),
    ]
    eng = make(clock, tmp_path, lambda n: script)
    events = await run(eng)
    started = [e for e in events if isinstance(e, ToolStarted)]
    finished = [e for e in events if isinstance(e, ToolFinished)]
    assert [(s.name, s.summary) for s in started] == [
        ("bash", "uv run pytest"),
        ("docs/search", "query=silk"),
    ]
    assert [f.status for f in finished] == ["success", "failure"]


async def test_tool_duration(clock, tmp_path):
    eng = make(clock, tmp_path, lambda n: [])
    client = await eng._ensure_client()
    eng._turn_id = "t"
    eng._queue = asyncio.Queue()
    await client.hook("PreToolUse", tool_name="x", tool_input={}, tool_use_id="a")
    await clock.advance(3)
    await client.hook(
        "PostToolUse", tool_name="x", tool_input={}, tool_response="", tool_use_id="a"
    )
    fin = [
        e for e in [eng._queue.get_nowait(), eng._queue.get_nowait()] if isinstance(e, ToolFinished)
    ]
    assert fin[0].duration_seconds == 3


async def test_interrupt_drains_stream_and_keeps_client(clock, tmp_path):
    script = [("pre", "mcp__sandbox__bash", {"command": "sleep 100"}, "tu1"), ("block",)]
    eng = make(clock, tmp_path, lambda n: script)
    collected: list = []

    async def consume():
        async for e in eng.run_turn(TurnRequest("conv", "t1", "go")):
            collected.append(e)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    assert [type(e) for e in collected] == [ToolStarted]
    await eng.interrupt()
    await asyncio.wait_for(task, 2)
    assert [type(e) for e in collected] == [ToolStarted, ToolFinished]
    assert collected[1].status == "interrupted"
    assert not any(isinstance(e, TurnFinished) for e in collected)
    # Same client is reused for the next turn (interrupt -> drain -> query).
    FakeSDKClient.instances[0].script = [("msg", text("again")), ("msg", result())]
    events = await run(eng, "next", "t2")
    assert isinstance(events[-1], TurnFinished)
    assert len(FakeSDKClient.instances) == 1
    assert FakeSDKClient.instances[0].queries == ["go", "next"]


async def test_append_sends_into_the_running_turn(clock, tmp_path):
    gate = asyncio.Event()

    class Gated(FakeSDKClient):
        async def receive_response(self):
            yield text("working")
            await gate.wait()
            yield result()

    eng = ClaudeAgentEngine(
        "conv",
        ClaudeEngineSettings(
            model="claude-haiku-5-5", project="nnyn-dev", region="global", work_root=tmp_path
        ),
        clock,
        client_factory=Gated,
    )
    assert await eng.append("too early") is False  # no turn running
    collected: list = []

    async def consume():
        async for e in eng.run_turn(TurnRequest("conv", "t1", "go")):
            collected.append(e)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    assert await eng.append("特に止まってない？") is True
    gate.set()
    await asyncio.wait_for(task, 2)
    assert isinstance(collected[-1], TurnFinished)
    assert FakeSDKClient.instances[-1].queries == ["go", "特に止まってない？"]
    assert await eng.append("after") is False  # the turn is over


async def test_prompt_appended_after_the_cli_turn_ended_is_still_answered(clock, tmp_path):
    """The CLI already finished its turn when the append arrived: it answers the prompt in
    a turn of its own. Both answers belong to our turn; one TurnFinished at the end."""
    appended = asyncio.Event()

    class LateAppend(FakeSDKClient):
        async def receive_messages(self):
            yield UserMessage(content="go")  # echo of the first prompt
            yield text("A でした")
            await appended.wait()  # the append is sent before the first result is read
            yield result(result="A でした")
            yield UserMessage(content="1+1 は？")  # consumed only now: a new CLI turn
            yield text("2 です")
            yield result(result="2 です")

    eng = ClaudeAgentEngine(
        "conv",
        ClaudeEngineSettings(
            model="claude-haiku-5-5", project="nnyn-dev", region="global", work_root=tmp_path
        ),
        clock,
        client_factory=LateAppend,
    )
    collected: list = []

    async def consume():
        async for e in eng.run_turn(TurnRequest("conv", "t1", "go")):
            collected.append(e)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    assert await eng.append("1+1 は？") is True
    appended.set()
    await asyncio.wait_for(task, 2)
    texts = [e.text for e in collected if isinstance(e, AssistantText)]
    assert texts == ["A でした", "2 です"]
    assert [type(e) for e in collected].count(TurnFinished) == 1
    assert isinstance(collected[-1], TurnFinished)


async def test_mirror_error_becomes_warning(clock, tmp_path):
    err = MirrorErrorMessage(subtype="mirror_error", data={}, key=None, error="boom")
    eng = make(clock, tmp_path, lambda n: [("msg", err), ("msg", text("x")), ("msg", result())])
    events = await run(eng)
    assert events[-1].warnings == ("会話履歴の一部が保存されていない可能性があります。",)


async def test_error_result_is_turn_failed(clock, tmp_path):
    eng = make(
        clock,
        tmp_path,
        lambda n: [("msg", result(is_error=True, subtype="error_max_turns", errors=["too many"]))],
    )
    events = await run(eng)
    assert isinstance(events[-1], TurnFailed) and "too many" in events[-1].error


async def test_exception_fails_turn_and_reconnects(clock, tmp_path):
    scripts = {1: [("raise", RuntimeError("cli died"))], 2: [("msg", result())]}
    eng = make(clock, tmp_path, lambda n: scripts[n])
    events = await run(eng)
    assert isinstance(events[-1], TurnFailed) and "cli died" in events[-1].error
    assert FakeSDKClient.instances[0].disconnected
    events = await run(eng, "retry", "t2")
    assert isinstance(events[-1], TurnFinished)
    assert len(FakeSDKClient.instances) == 2


async def test_options(clock, tmp_path):
    eng = make(
        clock,
        tmp_path,
        lambda n: [("msg", result())],
        mcp_servers=lambda key: {"docs": {"type": "http", "url": "https://x"}},
        system_prompt="sys",
    )
    await run_resume(eng, "saved-session")
    o = FakeSDKClient.instances[0].options
    assert o.model == "claude-haiku-5-5"
    assert o.env["CLAUDE_CODE_USE_VERTEX"] == "1"
    assert o.env["ANTHROPIC_VERTEX_PROJECT_ID"] == "nnyn-dev"
    assert o.env["CLOUD_ML_REGION"] == "global"
    assert set(HOST_TOOLS) <= set(o.disallowed_tools) and "Bash" in o.disallowed_tools
    assert o.strict_mcp_config is True
    assert o.setting_sources == ["project"]
    assert o.resume == "saved-session" and o.session_id is None
    assert set(o.hooks) == {"PreToolUse", "PostToolUse", "PostToolUseFailure"}
    assert Path(o.cwd).parent == tmp_path
    assert "ANTHROPIC_API_KEY" not in o.env


async def test_reset_starts_new_session(clock, tmp_path):
    eng = make(clock, tmp_path, lambda n: [("msg", result())])
    await run_resume(eng, "old")
    await eng.reset()
    await run(eng, "again", "t2")
    first, second = FakeSDKClient.instances
    assert first.disconnected and first.options.resume == "old"
    assert second.options.resume is None and second.options.session_id


async def test_session_id_is_reported(clock, tmp_path):
    seen = []
    eng = make(
        clock,
        tmp_path,
        lambda n: [("msg", result())],
        on_session_id=lambda k, s: seen.append((k, s)),
    )
    await run(eng)
    assert seen == [("conv", "sid-1")]


async def test_new_session_id_and_input_attempt_are_persisted_before_query(clock, tmp_path):
    persisted = []

    async def before_query(key, session_id, input_id, attempt_id):
        assert FakeSDKClient.instances[0].queries == []
        persisted.append((key, session_id, input_id, attempt_id))

    eng = make(
        clock,
        tmp_path,
        lambda n: [("msg", result())],
        on_query_start=before_query,
    )
    request = TurnRequest(
        "conv", "t1", "hello", input_id="input-1", dispatch_attempt_id="attempt-1"
    )
    _events = [event async for event in eng.run_turn(request)]
    options = FakeSDKClient.instances[0].options
    assert persisted == [("conv", options.session_id, "input-1", "attempt-1")]


async def test_notices_are_prepended(clock, tmp_path):
    eng = make(clock, tmp_path, lambda n: [("msg", result())])
    await run(eng, "続けて", notices=("作業環境は削除されました",))
    q = FakeSDKClient.instances[0].queries[0]
    assert q == "<system-notice>作業環境は削除されました</system-notice>\n続けて"


def test_summaries():
    assert display_name("mcp__docs__search") == "docs/search"
    assert display_name("mcp__sandbox__read_file") == "read_file"
    assert display_name("Bash") == "Bash"
    assert summarize("mcp__sandbox__read_file", {"path": "src/app.py"}) == "src/app.py"
    assert summarize("mcp__sandbox__grep", {"pattern": "TODO", "path": "src"}) == "TODO src"
    assert len(summarize("mcp__sandbox__bash", {"command": "x" * 500})) <= 120


class StaticSkills:
    def __init__(self, generation):
        self.generation = generation

    def current(self):
        return self.generation

    async def refresh(self):
        return self.generation


async def test_skill_update_applies_from_next_turn(clock, tmp_path):
    from silk_agent.core.ports import SkillGeneration

    def gen(name, body):
        root = tmp_path / "gens" / name
        (root / ".claude/skills/report").mkdir(parents=True)
        (root / ".claude/skills/report/SKILL.md").write_text(body)
        return SkillGeneration(name, root)

    skills = StaticSkills(gen("g1", "v1"))
    copied = []

    async def on_gen(key, g):
        copied.append((key, g.generation_id))

    eng = make(
        clock,
        tmp_path / "work",
        lambda n: [("msg", result())],
        skills=skills,
        on_skills_generation=on_gen,
    )
    await run(eng)
    c1 = FakeSDKClient.instances[0]
    assert (Path(c1.options.cwd) / ".claude/skills/report/SKILL.md").read_text() == "v1"
    assert copied == [("conv", "g1")]

    # Same generation: client is reused and nothing is copied again.
    await run(eng, "again", "t2")
    assert len(FakeSDKClient.instances) == 1 and copied == [("conv", "g1")]

    # A sync happens between turns -> the next turn reconnects (same session) on g2.
    skills.generation = gen("g2", "v2")
    await run(eng, "after update", "t3")
    assert len(FakeSDKClient.instances) == 2
    c2 = FakeSDKClient.instances[1]
    assert c1.disconnected
    assert (Path(c2.options.cwd) / ".claude/skills/report/SKILL.md").read_text() == "v2"
    assert c2.options.resume == "sid-1"
    assert copied == [("conv", "g1"), ("conv", "g2")]


async def test_tool_hook_events_carry_the_raw_input_and_result(clock, tmp_path):
    seen = []
    eng = make(clock, tmp_path, lambda n: [], on_hook_event=seen.append)
    client = await eng._ensure_client()
    eng._turn_id = "t1"
    eng._queue = asyncio.Queue()
    await client.hook(
        "PreToolUse",
        tool_name="mcp__sandbox__bash",
        tool_input={"command": "ls -la\nsecond"},
        tool_use_id="a",
    )
    await clock.advance(2)
    await client.hook(
        "PostToolUse",
        tool_name="mcp__sandbox__bash",
        tool_input={},
        tool_response="total 0",
        tool_use_id="a",
    )
    await client.hook(
        "PreToolUse", tool_name="mcp__docs__search", tool_input={"query": "x"}, tool_use_id="b"
    )
    await client.hook(
        "PostToolUseFailure",
        tool_name="mcp__docs__search",
        tool_input={},
        error="not found",
        tool_use_id="b",
        is_interrupt=False,
    )
    start, end, start2, end2 = seen
    assert (start.kind, start.tool_name, start.tool_use_id, start.turn_id) == (
        "tool_start",
        "mcp__sandbox__bash",
        "a",
        "t1",
    )
    assert start.tool_input == {"command": "ls -la\nsecond"}  # raw, not the one-line summary
    assert (end.kind, end.status, end.tool_response, end.duration_seconds) == (
        "tool_end",
        "success",
        "total 0",
        2.0,
    )
    assert end.tool_name == "mcp__sandbox__bash"
    assert (end2.status, end2.tool_response) == ("failure", "not found")
    assert start2.conversation_key == "conv"


async def test_a_background_tool_is_reported_as_background(clock, tmp_path):
    from silk_agent.agent.sandbox_tools import BACKGROUND_MARKER

    seen = []
    eng = make(clock, tmp_path, lambda n: [], on_hook_event=seen.append)
    client = await eng._ensure_client()
    eng._queue = asyncio.Queue()
    await client.hook("PreToolUse", tool_name="x", tool_input={}, tool_use_id="a")
    await client.hook(
        "PostToolUse",
        tool_name="x",
        tool_input={},
        tool_response=f"{BACKGROUND_MARKER} cmd",
        tool_use_id="a",
    )
    assert seen[1].status == "background"


async def test_interrupted_tools_are_reported_as_ended(clock, tmp_path):
    seen = []
    script = [("pre", "mcp__sandbox__bash", {"command": "sleep 100"}, "tu1"), ("block",)]
    eng = make(clock, tmp_path, lambda n: script, on_hook_event=seen.append)

    async def consume():
        async for _ in eng.run_turn(TurnRequest("conv", "t1", "go")):
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    await eng.interrupt()
    await asyncio.wait_for(task, 2)
    assert [(e.kind, getattr(e, "status", None)) for e in seen] == [
        ("tool_start", None),
        ("tool_end", "interrupted"),
    ]


async def test_a_failing_hook_handler_does_not_break_the_turn(clock, tmp_path):
    def boom(event):
        raise RuntimeError("handler bug")

    script = [
        ("pre", "mcp__sandbox__bash", {"command": "ls"}, "tu1"),
        ("post", "mcp__sandbox__bash", "tu1"),
        ("msg", result()),
    ]
    eng = make(clock, tmp_path, lambda n: script, on_hook_event=boom)
    events = await run(eng)
    assert any(isinstance(e, TurnFinished) for e in events)


class FakeHistoryStore:
    """The parts of AgentPlatformSessionStore the engine uses to guard a conversation's history."""

    def __init__(self, existing=(), loss=()):
        self.existing = set(existing)
        self.loss = set(loss)
        self.extended: list[tuple[str, str | None]] = []
        self.checked: list[str] = []

    async def session_exists(self, session_id):
        self.checked.append(session_id)
        return session_id in self.existing

    async def extend_for_turn(self, conversation_key, session_id):
        # Runs before the SDK sees this turn's input: it has been sent `len(extended)` inputs.
        assert sum(len(c.queries) for c in FakeSDKClient.instances) == len(self.extended)
        self.extended.append((conversation_key, session_id))

    def take_history_loss(self, session_id):
        if session_id in self.loss:
            self.loss.discard(session_id)
            return True
        return False


async def test_history_is_extended_before_the_input_reaches_the_sdk(clock, tmp_path):
    store = FakeHistoryStore(existing={"old"})
    eng = make(clock, tmp_path, lambda n: [("msg", result())], session_store=store)
    events = await run_resume(eng, "old")
    assert [type(e) for e in events] == [TurnFinished]
    assert store.extended == [("conv", "old")]
    assert FakeSDKClient.instances[0].options.resume == "old"
    assert eng.history_reset_reason is None


async def test_a_new_conversation_has_nothing_to_extend_or_check(clock, tmp_path):
    store = FakeHistoryStore()
    eng = make(clock, tmp_path, lambda n: [("msg", result())], session_store=store)
    await run(eng)
    assert store.checked == [] and store.extended == []


async def test_each_turn_extends_the_history_but_checks_existence_only_when_connecting(
    clock, tmp_path
):
    store = FakeHistoryStore(existing={"old", "sid-1"})
    eng = make(clock, tmp_path, lambda n: [("msg", result())], session_store=store)
    await run_resume(eng, "old")
    await run(eng, "second", "t2")
    assert store.checked == ["old"]
    assert len(store.extended) == 2


async def test_missing_history_starts_a_new_session_and_tells_the_user(clock, tmp_path):
    store = FakeHistoryStore(existing=set())
    persisted = []

    async def before_query(key, session_id, input_id, attempt_id):
        persisted.append(session_id)

    eng = make(
        clock,
        tmp_path,
        lambda n: [("msg", text("はい")), ("msg", result())],
        session_store=store,
        on_query_start=before_query,
    )
    events = await run_resume(eng, "gone")
    assert [type(e) for e in events] == [Notice, AssistantText, TurnFinished]
    assert events[0] == Notice("conv", HISTORY_LOST_NOTICE)
    options = FakeSDKClient.instances[0].options
    assert options.resume is None and options.session_id
    assert persisted == [options.session_id]  # the conversation's sdk_session_id moves on
    assert eng.history_reset_reason == "expired"
    assert store.extended == []  # nothing left to extend


async def test_history_lost_while_appending_is_recorded(clock, tmp_path):
    store = FakeHistoryStore(existing={"old"}, loss={"sid-1"})
    eng = make(clock, tmp_path, lambda n: [("msg", result())], session_store=store)
    events = await run_resume(eng, "old")
    assert [type(e) for e in events] == [TurnFinished]  # no notice: the turn had its history
    assert eng.history_reset_reason == "expired"


async def test_reset_forgets_the_history_reset_reason(clock, tmp_path):
    store = FakeHistoryStore(existing=set())
    eng = make(clock, tmp_path, lambda n: [("msg", result())], session_store=store)
    await run_resume(eng, "gone")
    assert eng.history_reset_reason == "expired"
    await eng.reset()
    assert eng.history_reset_reason is None


async def test_a_store_without_history_support_is_left_alone(clock, tmp_path):
    eng = make(clock, tmp_path, lambda n: [("msg", result())], session_store=object())
    events = await run_resume(eng, "old")
    assert [type(e) for e in events] == [TurnFinished]
    assert FakeSDKClient.instances[0].options.resume == "old"


async def test_history_created_after_a_reset_is_still_extended(clock, tmp_path):
    store = FakeHistoryStore(existing={"old", "sid-1"})
    eng = make(clock, tmp_path, lambda n: [("msg", result())], session_store=store)
    await run_resume(eng, "old")
    await eng.reset()
    await run(eng, "after reset", "t2")  # starts a new SDK session; "old" is not resumed
    assert FakeSDKClient.instances[-1].options.resume is None
    await run(eng, "next", "t3")
    assert eng.session_id == "sid-1"
    assert store.extended[-1] == ("conv", "sid-1")
    assert all(sid != "old" for _, sid in store.extended[1:])
