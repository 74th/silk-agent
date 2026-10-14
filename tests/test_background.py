import asyncio

from silk_agent.agent.sandbox_tools import (
    BACKGROUND_MARKER,
    FOREGROUND_SECONDS,
    build_sandbox_tools,
)
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


async def test_default_foreground_window_is_twenty_seconds(monkeypatch):
    rt = SlowRuntime()
    rt.release("quick command")
    waited = []
    original_wait = BackgroundCommands.wait

    async def record_wait(background, entry, seconds):
        waited.append(seconds)
        return await original_wait(background, entry, seconds)

    monkeypatch.setattr(BackgroundCommands, "wait", record_wait)
    bash = {t.name: t.handler for t in build_sandbox_tools("K", rt)}["bash"]

    result = await bash({"command": "quick command"})

    assert FOREGROUND_SECONDS == 20.0
    assert waited == [20.0]
    assert "exit code: 0" in body(result)


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


async def test_distributed_tools_use_persistent_command_ledger():
    class Monitor:
        async def detach(self, command_id):
            return {"decision": "background", "command_id": command_id}

        async def get_command(self, command_id):
            return {"command_id": command_id, "state": "running"}

        async def command_output(self, command_id, *, cursor, limit_bytes=65536):
            return "archived output", cursor + 15

        async def cancel(self, command_id, *, reason, send_cancel):
            outcome = await send_cancel("K", command_id, "stable-request")
            return {"command_id": command_id, "cancel_outcome": outcome}

    async def send_cancel(_key, _command_id, request_id):
        assert request_id == "stable-request"
        return "accepted"

    class RemoteWaitRuntime(SlowRuntime):
        async def run_command(
            self, key, command, *, cwd=None, timeout_seconds=None, on_started=None
        ):
            cid = f"cmd-{len(self.sent_commands) + 1}"
            self.sent_commands.append((key, command))
            if on_started is not None:
                await on_started(cid)
            try:
                await self.gates.setdefault(command, asyncio.Event()).wait()
            except asyncio.CancelledError:
                # Cancelling the HTTP wait is distinct from cancelling the remote command.
                raise
            return CommandResult(cid, "completed", 0)

    runtime = RemoteWaitRuntime()
    background = BackgroundCommands(runtime, command_monitor=Monitor(), cancel_sender=send_cancel)
    tools = {
        item.name: item.handler
        for item in build_sandbox_tools(
            "K", runtime, background=background, foreground_seconds=0.01
        )
    }
    started = await tools["bash"]({"command": "make build", "run_in_background": True})
    assert BACKGROUND_MARKER in body(started)
    assert "outcome: running" in body(await tools["command_status"]({"command_id": "cmd-1"}))
    output = await tools["command_output"]({"command_id": "cmd-1"})
    assert "archived output" in body(output) and "next_cursor: 15" in body(output)
    cancelled = await tools["cancel_command"]({"command_id": "cmd-1"})
    assert "停止要求を受け付けました" in body(cancelled)
    assert runtime.cancelled == []


async def test_distributed_stop_cancels_persisted_commands_and_reports_unknown():
    class Monitor:
        def __init__(self):
            self.cursors = []
            self.cancel_requests = []

        async def list_for_stop(self, **kwargs):
            self.cursors.append(kwargs["cursor"])
            if kwargs["cursor"] is None:
                return ({"command_id": "cmd-1"},), "next-page"
            return ({"command_id": "cmd-2"},), None

        async def cancel(self, command_id, *, reason, send_cancel):
            outcome = await send_cancel("K", command_id, f"request-{command_id}")
            self.cancel_requests.append((command_id, reason, outcome))
            return {"cancel_outcome": outcome}

    sent = []

    async def send_cancel(key, command_id, request_id):
        sent.append((key, command_id, request_id))
        return "unknown"

    monitor = Monitor()
    background = BackgroundCommands(
        InMemoryRuntime(), command_monitor=monitor, cancel_sender=send_cancel
    )
    report = await background.cancel_all_for_stop(
        "K", conversation_generation=3, stop_seq=18, page_size=1
    )
    assert report.requested == 2 and report.unknown == 2 and report.confirmed == 0
    assert monitor.cursors == [None, "next-page"]
    assert monitor.cancel_requests == [
        ("cmd-1", "user_stop", "unknown"),
        ("cmd-2", "user_stop", "unknown"),
    ]
    assert [item[1] for item in sent] == ["cmd-1", "cmd-2"]


async def test_worker_shutdown_orphans_foreground_command_without_remote_cancel():
    class Monitor:
        def __init__(self):
            self.orphaned = []

        async def orphan(self, command_id):
            self.orphaned.append(command_id)
            return {"decision": "orphaned"}

    class RemoteWaitRuntime(SlowRuntime):
        async def run_command(
            self, key, command, *, cwd=None, timeout_seconds=None, on_started=None
        ):
            command_id = "cmd-foreground"
            if on_started is not None:
                await on_started(command_id)
            try:
                await self.gates.setdefault(command, asyncio.Event()).wait()
            except asyncio.CancelledError:
                raise
            return CommandResult(command_id, "completed")

    runtime, monitor = RemoteWaitRuntime(), Monitor()
    background = BackgroundCommands(runtime, command_monitor=monitor)
    entry = await background.start("K", "compile")
    assert entry.command_id == "cmd-foreground"
    assert await background.orphan_all("K") == 1
    assert monitor.orphaned == ["cmd-foreground"]
    assert runtime.cancelled == []


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
