import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from gke_session_sandbox.errors import FileOperationError
from silk_agent.core.events import TurnFinished, UserMessage
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.stopwords import StopWords
from silk_agent.sandbox.gke import GkeSessionSandboxRuntime, to_session_key_parts
from silk_agent.testing.fakes import (
    AllowAllGuard,
    FakeAgentEngine,
    NoopSanitizer,
    ScriptedChannel,
    Text,
)


class ExecutionOutcomeUnknown(Exception):
    def __init__(self, command_id=""):
        super().__init__("unknown")
        self.command_id = command_id


class StaleGenerationError(Exception):
    pass


class FakeCommands:
    """Mirrors gke_session_sandbox.CommandTrackerClient (start / wait / get / read_output /
    cancel)."""

    def __init__(self):
        self.runs = []  # (argv, cwd, timeout_seconds, command_id)
        self.mode = "ok"
        self.gate = asyncio.Event()
        self.cancelled = []

    async def start(self, argv, *, command_id, cwd="", timeout_seconds=0):
        self.runs.append((argv, cwd, timeout_seconds, command_id))
        if self.mode == "unknown":
            raise ExecutionOutcomeUnknown(command_id)
        return {"command_id": command_id, "state": "running"}

    async def wait(self, command_id):
        if self.mode == "block":
            await self.gate.wait()
            return SimpleNamespace(
                command_id=command_id,
                exit_code=-15,
                stdout=b"",
                stderr=b"",
                stop_reason="cancelled",
            )
        return SimpleNamespace(
            command_id=command_id, exit_code=0, stdout=b"hello\n", stderr=b"", stop_reason=None
        )

    async def cancel(self, command_id):
        self.cancelled.append(command_id)
        self.gate.set()

    async def get(self, command_id):
        cancelled = command_id in self.cancelled
        return {
            "command_id": command_id,
            "state": "finished" if cancelled else "running",
            "stop_reason": "cancelled" if cancelled else None,
            "exit_code": None,
        }

    async def read_output(self, command_id, *, stream="stdout", cursor=0):
        return SimpleNamespace(data=b"out", cursor=cursor + 3)


class FakeFiles:
    def __init__(self):
        self.data = {}

    async def read(self, path):
        if path not in self.data:
            raise FileOperationError("missing", path=path, status=404)
        return self.data[path]

    async def write(self, path, data):
        self.data[path] = data

    async def list(self, path):
        return [SimpleNamespace(path=p, type="file", size=len(d)) for p, d in self.data.items()]


class FakeHandle:
    def __init__(self, key, events=()):
        self.key = key
        self.commands = FakeCommands()
        self.files = FakeFiles()
        self.pending_context_events = tuple(events)


class FakeManager:
    def __init__(self):
        self.ensured = []
        self.handles = {}
        self.events = {}  # (tenant, session) -> [events]
        self.acked = []

    async def ensure(self, key, env=None):
        self.ensured.append((key, env))
        k = (key[0], key[1])
        if k not in self.handles:
            self.handles[k] = FakeHandle(key, self.events.get(k, ()))
        return self.handles[k]

    async def ack_context_event(self, key, event_id):
        self.acked.append((key, event_id))

    async def close(self):
        pass


def make(env=None):
    mgr = FakeManager()

    async def env_provider(key):
        return env or {}

    rt = GkeSessionSandboxRuntime(
        mgr, env_provider=env_provider, session_key_factory=lambda t, s: (t, s)
    )
    return rt, mgr


def test_session_key_mapping():
    assert to_session_key_parts("slack:T1:C1:100.1") == ("T1", "C1:100.1")
    assert to_session_key_parts("slack:T1:D1:-") == ("T1", "D1:-")
    assert to_session_key_parts("streamlit:abc") == ("streamlit", "abc")


async def test_ensure_with_user_email_env_and_command_mapping():
    rt, mgr = make({"AGENT_USER_EMAIL": "taro@example.com"})
    started = []

    async def on_started(cid):
        started.append(cid)

    res = await rt.run_command("slack:T1:C1:1.0", "ls -la", cwd="src", on_started=on_started)
    assert mgr.ensured[0] == (("T1", "C1:1.0"), {"AGENT_USER_EMAIL": "taro@example.com"})
    argv, cwd, timeout, command_id = mgr.handles[("T1", "C1:1.0")].commands.runs[0]
    assert argv == ["bash", "-lc", "ls -la"] and cwd == "src" and timeout == 120
    assert (res.outcome, res.exit_code, res.stdout, res.command_id) == (
        "completed",
        0,
        "hello\n",
        command_id,
    )
    assert started == [command_id]


async def test_unknown_outcome_is_returned_with_id_and_never_resent():
    rt, mgr = make()
    await rt.run_command("slack:T1:C1:1.0", "echo warm")
    cmds = mgr.handles[("T1", "C1:1.0")].commands
    cmds.mode = "unknown"
    seen = []

    async def on_started(cid):
        seen.append(cid)

    res = await rt.run_command("slack:T1:C1:1.0", "make deploy", on_started=on_started)
    sent_id = cmds.runs[-1][3]
    assert (res.outcome, res.command_id) == ("unknown", sent_id)
    assert seen == [sent_id]
    assert [r[0][2] for r in cmds.runs] == ["echo warm", "make deploy"]  # exactly one send


async def test_conversations_use_separate_sessions_and_files_are_isolated():
    rt, mgr = make()
    await rt.write_file("slack:T1:C1:1.0", "a.txt", b"secret")
    assert await rt.read_file("slack:T1:C1:1.0", "a.txt") == b"secret"
    with pytest.raises(FileNotFoundError):
        await rt.read_file("slack:T1:C2:2.0", "a.txt")
    assert [e.path for e in await rt.list_files("slack:T1:C1:1.0", ".")] == ["a.txt"]
    assert len(mgr.handles) == 2


async def test_stale_generation_ensures_again_and_retries_once():
    rt, mgr = make()
    key = "slack:T1:C1:1.0"
    await rt.write_file(key, "a", b"1")
    old = mgr.handles[("T1", "C1:1.0")]

    async def stale(*a, **k):
        raise StaleGenerationError()

    old.files.write = stale
    mgr.handles.pop(("T1", "C1:1.0"))  # a new generation will be created by ensure()
    await rt.write_file(key, "b", b"2")
    assert mgr.handles[("T1", "C1:1.0")].files.data == {"b": b"2"}
    assert len(mgr.ensured) == 2


async def test_cancel_commands_cancels_the_running_command():
    rt, mgr = make()
    key = "slack:T1:C1:1.0"
    await rt.run_command(key, "warm")
    cmds = mgr.handles[("T1", "C1:1.0")].commands
    cmds.mode = "block"
    task = asyncio.create_task(rt.run_command(key, "sleep 100"))
    await asyncio.sleep(0.05)
    await rt.cancel_commands(key)
    res = await asyncio.wait_for(task, 2)
    assert res.outcome == "cancelled" and cmds.cancelled == [cmds.runs[-1][3]]


async def test_cancelling_the_waiting_tool_leaves_the_sandbox_command_running():
    # Wait cancellation does not imply an explicit remote command cancellation.
    rt, mgr = make()
    key = "slack:T1:C1:1.0"
    await rt.run_command(key, "warm")
    cmds = mgr.handles[("T1", "C1:1.0")].commands
    cmds.mode = "block"
    started = []

    async def on_started(command_id):
        started.append(command_id)

    task = asyncio.create_task(rt.run_command(key, "sleep 300 && echo done", on_started=on_started))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    command_id = cmds.runs[-1][3]
    assert started == [command_id]
    assert cmds.cancelled == []
    assert (await rt.command_status(key, command_id)).outcome == "running"

    await rt.cancel_commands(key)
    assert cmds.cancelled == [command_id]
    assert (await rt.command_status(key, command_id)).outcome == "cancelled"


async def test_status_and_output():
    rt, _ = make()
    st = await rt.command_status("slack:T1:C1:1.0", "cmd-1")
    assert st.outcome == "running"
    assert await rt.command_output("slack:T1:C1:1.0", "cmd-1", 0) == ("out", 3)
    assert await rt.command_output("slack:T1:C1:1.0", "cmd-1", 3) == ("out", 6)


async def test_copy_skills_writes_under_dot_skills(tmp_path: Path):
    skills = tmp_path / ".claude" / "skills" / "report"
    skills.mkdir(parents=True)
    (skills / "SKILL.md").write_text("hello")
    rt, mgr = make()
    await rt.copy_skills("slack:T1:C1:1.0", tmp_path, "g1")
    assert mgr.handles[("T1", "C1:1.0")].files.data == {".skills/report/SKILL.md": b"hello"}


# ------------------------------------------------ notices reach the next turn


async def test_notice_reaches_the_next_turn_and_is_acked_afterwards(clock):
    rt, mgr = make()
    key = "slack:T1:C1:1.0"
    event = SimpleNamespace(
        event_id="ev-1",
        context_message="作業環境は保持期限で削除されました。前回のファイルは使えません。",
    )
    mgr.events[("T1", "C1:1.0")] = [event]
    ch, engines = ScriptedChannel(), {}

    def factory(k):
        engines[k] = FakeAgentEngine(clock, [Text("了解")])
        return engines[k]

    orch = ConversationOrchestrator(
        factory,
        ch,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(["停止"]),
        notice_source=rt.pop_notices,
        notice_ack=rt.ack_notices,
    )
    await orch.handle(UserMessage(key, "u", "続きをお願い"))
    await ch.wait_for(TurnFinished)
    request = engines[key].requests[0]
    assert request.notices == ("作業環境は保持期限で削除されました。前回のファイルは使えません。",)
    assert mgr.acked == [(("T1", "C1:1.0"), "ev-1")]
    # acked: the next turn carries no notice
    mgr.handles[("T1", "C1:1.0")].pending_context_events = ()
    await orch.handle(UserMessage(key, "u", "もう一度"))
    await ch.wait_for(TurnFinished, count=2)
    assert engines[key].requests[1].notices == ()
