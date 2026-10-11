"""Conversation continuity across a Bot restart, with the real Agent Platform Sessions store
and Claude (claude-haiku-5-5). Run with `-m integration`."""

import asyncio
import uuid

import pytest

from silk_agent.config import Config
from silk_agent.core.events import AssistantText, ToolStarted, TurnFailed, TurnFinished
from silk_agent.core.ports import TurnRequest
from silk_agent.sandbox.local import LocalProcessRuntime
from silk_agent.stores.agent_platform import AgentPlatformSessions, AgentPlatformSessionStore
from silk_agent.testing.fakes import SystemClock
from silk_agent.wiring import build_engine_factory
from tests.integration.conftest import PROJECT

pytestmark = pytest.mark.integration


def _config() -> Config:
    config = Config()
    config.agent.project = PROJECT
    return config


def engine_for(tmp_path, sessions_api, key, process: str):
    """A new `process`: fresh CLAUDE_CONFIG_DIR, so nothing local survives the "restart"."""
    config_dir = tmp_path / f"claude-config-{process}"
    config_dir.mkdir(exist_ok=True)
    factory = build_engine_factory(
        _config(),
        LocalProcessRuntime(tmp_path / "sandbox"),
        SystemClock(),
        work_root=tmp_path / "work",  # same work_root -> same cwd -> same SessionStore key
        session_store=AgentPlatformSessionStore(sessions_api),
        model="claude-haiku-5-5",
        extra_env={"CLAUDE_CONFIG_DIR": str(config_dir)},
    )
    return factory(key)


async def turn(engine, key, text, resume=None, turn_id=None, notices=()):
    request = TurnRequest(key, turn_id or uuid.uuid4().hex[:8], text, notices, resume)
    return [e async for e in engine.run_turn(request)]


def answer(events):
    return "".join(e.text for e in events if isinstance(e, AssistantText))


async def test_conversation_continues_after_restart(tmp_path, sessions_api: AgentPlatformSessions):
    key = f"resume-test:{uuid.uuid4().hex}"
    first = engine_for(tmp_path, sessions_api, key, "before")
    events = await asyncio.wait_for(
        turn(first, key, "数字の42を覚えてください。「覚えました」とだけ答えて。"), 180
    )
    assert isinstance(events[-1], TurnFinished), events
    session_id = first.session_id
    assert session_id
    await first.close()
    await asyncio.sleep(2)  # let the store mirror settle

    second = engine_for(tmp_path, sessions_api, key, "after")
    try:
        events = await asyncio.wait_for(
            turn(
                second, key, "覚えてもらった数字は何でしたか？数字だけ答えて。", resume=session_id
            ),
            180,
        )
    finally:
        await second.close()
    assert isinstance(events[-1], TurnFinished), events
    assert "42" in answer(events)


async def test_resume_from_transcript_cut_inside_tool_use(
    tmp_path, sessions_api: AgentPlatformSessions
):
    key = f"cut-test:{uuid.uuid4().hex}"
    first = engine_for(tmp_path, sessions_api, key, "before")
    seen: list = []

    async def consume():
        async for e in first.run_turn(
            TurnRequest(
                key, "t1", "数字の7を覚えてから、bash ツールで `sleep 300` を実行してください。"
            )
        ):
            seen.append(e)

    task = asyncio.create_task(consume())
    for _ in range(240):
        if any(isinstance(e, ToolStarted) and e.name == "bash" for e in seen):
            break
        await asyncio.sleep(0.5)
    assert any(isinstance(e, ToolStarted) and e.name == "bash" for e in seen), seen
    await asyncio.sleep(4)  # the tool_use entry reaches the store (eager flush)
    # Simulated crash: kill the CLI without any interrupt / result.
    client = first._client
    assert client is not None
    options_session = client.options.session_id
    await client.disconnect()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    second = engine_for(tmp_path, sessions_api, key, "after")
    try:
        events = await asyncio.wait_for(
            turn(
                second,
                key,
                "続けて。最初に覚えてもらった数字は何でしたか？数字だけ答えて。",
                resume=options_session,
                notices=("前回のturnはBotの停止により途中で中断されました。",),
            ),
            180,
        )
    finally:
        await second.close()
    failed = [e for e in events if isinstance(e, TurnFailed)]
    assert not failed, failed
    assert isinstance(events[-1], TurnFinished), events
    assert "7" in answer(events)


class AlwaysFailingStore:
    async def append(self, key, entries):
        raise RuntimeError("session store is down")

    async def load(self, key):
        return None


async def test_failing_store_adds_warning_to_turn(tmp_path):
    factory = build_engine_factory(
        _config(),
        LocalProcessRuntime(tmp_path / "sandbox"),
        SystemClock(),
        work_root=tmp_path / "work",
        session_store=AlwaysFailingStore(),
        model="claude-haiku-5-5",
        extra_env={"CLAUDE_CONFIG_DIR": str(tmp_path / "cfg")},
    )
    engine = factory("warn-test")
    try:
        events = await asyncio.wait_for(turn(engine, "warn-test", "「はい」とだけ答えて。"), 180)
    finally:
        await engine.close()
    assert isinstance(events[-1], TurnFinished), events
    assert events[-1].warnings == ("会話履歴の一部が保存されていない可能性があります。",)
