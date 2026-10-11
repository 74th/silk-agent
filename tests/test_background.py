import asyncio

from silk_agent.agent.sandbox_tools import BACKGROUND_MARKER, build_sandbox_tools
from silk_agent.core.background import BackgroundCommands
from silk_agent.core.events import Notice, TurnFinished, UserMessage
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.ports import CommandResult
from silk_agent.core.session import SYSTEM_USER, SessionSettings
from silk_agent.core.stopwords import StopWords
from silk_agent.testing.fakes import (
    AllowAllGuard,
    FakeAgentEngine,
    InMemoryRuntime,
    NoopSanitizer,
    ScriptedChannel,
    Text,
    Tool,
)


class SlowRuntime(InMemoryRuntime):
    """run_command blocks until `release(command)`; cancelling it stops the command."""

    def __init__(self):
        super().__init__()
        self.gates: dict[str, asyncio.Event] = {}
        self.cancelled: list[str] = []

    def release(self, command):
        self.gates.setdefault(command, asyncio.Event()).set()

    async def run_command(self, key, command, *, cwd=None, timeout_seconds=None, on_started=None):
        cid = f"cmd-{len(self.sent_commands) + 1}"
        self.sent_commands.append((key, command))
        if on_started is not None:
            await on_started(cid)
        gate = self.gates.setdefault(command, asyncio.Event())
        try:
            await gate.wait()
        except asyncio.CancelledError:
            self.cancelled.append(cid)
            raise
        return CommandResult(cid, "completed", 0, stdout=f"{command} done\n")


def body(res):
    return res["content"][0]["text"]


async def test_quick_command_returns_its_result():
    rt = SlowRuntime()
    rt.release("ls")
    bash = {t.name: t.handler for t in build_sandbox_tools("K", rt, foreground_seconds=1)}["bash"]
    res = await bash({"command": "ls"})
    assert "exit code: 0" in body(res) and "ls done" in body(res)


async def test_long_command_moves_to_background_and_reports_completion():
    rt = SlowRuntime()
    bg = BackgroundCommands(rt)
    notices = []

    async def on_finished(key, text):
        notices.append((key, text))

    bg.on_finished = on_finished
    tools = {t.name: t.handler for t in build_sandbox_tools("K", rt, None, bg, 0.05)}
    res = await tools["bash"]({"command": "sleep 300 && echo done"})
    assert BACKGROUND_MARKER in body(res) and "command_id=cmd-1" in body(res)
    assert [e.command for e in bg.running("K")] == ["sleep 300 && echo done"]
    status = await tools["command_status"]({"command_id": "cmd-1"})
    assert "running" in body(status)

    rt.release("sleep 300 && echo done")
    for _ in range(50):
        if notices:
            break
        await asyncio.sleep(0.01)
    key, text = notices[0]
    assert key == "K" and "command_id=cmd-1" in text and "exit code: 0" in text
    assert "sleep 300 && echo done done" in text
    assert bg.running("K") == []


async def test_run_in_background_returns_immediately():
    rt = SlowRuntime()
    tools = {t.name: t.handler for t in build_sandbox_tools("K", rt, foreground_seconds=60)}
    res = await asyncio.wait_for(
        tools["bash"]({"command": "make build", "run_in_background": True}), 1
    )
    assert BACKGROUND_MARKER in body(res)


async def test_cancel_command_stops_it_without_a_completion_notice():
    rt = SlowRuntime()
    bg = BackgroundCommands(rt)
    notices = []

    async def on_finished(key, text):
        notices.append(text)

    bg.on_finished = on_finished
    tools = {t.name: t.handler for t in build_sandbox_tools("K", rt, None, bg, 0.01)}
    await tools["bash"]({"command": "sleep 300"})
    res = await tools["cancel_command"]({"command_id": "cmd-1"})
    assert "止めました" in body(res)
    assert rt.cancelled == ["cmd-1"] and bg.running("K") == []
    await asyncio.sleep(0.02)
    assert notices == []


async def test_cancelled_tool_call_keeps_the_command_running():
    # An interrupted turn cancels the tool call, not the background command.
    rt = SlowRuntime()
    bg = BackgroundCommands(rt)
    tools = {t.name: t.handler for t in build_sandbox_tools("K", rt, None, bg, 60)}
    call = asyncio.create_task(tools["bash"]({"command": "sleep 300"}))
    await asyncio.sleep(0.02)
    call.cancel()
    await asyncio.sleep(0.02)
    assert rt.cancelled == [] and len(bg.running("K")) == 1


# ---------------------------------------------------------------- conversation


def make_orch(clock, rt, scenario):
    channel = ScriptedChannel()
    bg = BackgroundCommands(rt)
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
        cancel_background=bg.cancel_all,
    )
    bg.on_finished = orch.notify
    return orch, channel, bg, engines


async def test_completion_starts_a_turn_when_the_conversation_is_idle(clock):
    rt = SlowRuntime()
    orch, channel, bg, engines = make_orch(clock, rt, lambda req: [Text("reply:" + req.text)])
    await orch.handle(UserMessage("K", "u", "ビルドして"))
    await channel.wait_for(TurnFinished)
    entry = await bg.start("K", "make build")
    assert bg.detach(entry) is None
    rt.release("make build")
    await channel.wait_for(TurnFinished, count=2)
    request = engines["K"].requests[1]
    assert "command_id=cmd-1" in request.text and "make build done" in request.text


async def test_completion_joins_the_running_turn(clock):
    rt = SlowRuntime()
    orch, channel, bg, engines = make_orch(clock, rt, [Tool("bash", duration=100)])
    await orch.handle(UserMessage("K", "u", "別の作業"))
    await asyncio.sleep(0.01)
    entry = await bg.start("K", "make build")
    bg.detach(entry)
    rt.release("make build")
    for _ in range(50):
        if engines["K"].appended:
            break
        await asyncio.sleep(0.01)
    assert "command_id=cmd-1" in engines["K"].appended[0]
    assert len(engines["K"].requests) == 1  # no extra turn, no interruption


async def test_stop_word_stops_background_commands_while_idle(clock):
    rt = SlowRuntime()
    orch, channel, bg, _ = make_orch(clock, rt, [Text("ok")])
    entry = await bg.start("K", "sleep 300")
    bg.detach(entry)
    await orch.handle(UserMessage("K", "u", "停止"))
    assert rt.cancelled == ["cmd-1"]
    assert [n.text for n in channel.of(Notice)] == [
        "バックグラウンドで実行中のコマンド1件を停止しました。"
    ]


def test_system_user_name():
    assert SYSTEM_USER == "system"
