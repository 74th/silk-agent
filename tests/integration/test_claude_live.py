"""Runs the real agent on Agent Platform (claude-haiku-5-5) with LocalProcessRuntime.
Run with `-m integration`."""

import asyncio
from pathlib import Path

import pytest

from silk_agent.config import Config
from silk_agent.core.events import (
    AssistantText,
    ToolFinished,
    ToolStarted,
    TurnFailed,
    TurnFinished,
)
from silk_agent.core.ports import TurnRequest
from silk_agent.sandbox.local import LocalProcessRuntime
from silk_agent.testing.fakes import SystemClock
from silk_agent.wiring import build_engine_factory

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
MODEL = "claude-haiku-5-5"


@pytest.fixture
def setup(tmp_path):
    config = Config()
    runtime = LocalProcessRuntime(tmp_path / "sandbox")
    factory = build_engine_factory(
        config, runtime, SystemClock(), root=ROOT, work_root=tmp_path / "work", model=MODEL
    )
    return runtime, factory


async def collect(engine, text, turn="t1"):
    return [e async for e in engine.run_turn(TurnRequest("live", turn, text))]


async def test_tool_call_and_events(setup):
    runtime, factory = setup
    engine = factory("live")
    try:
        events = await asyncio.wait_for(
            collect(
                engine,
                "bash ツールで `echo silk-agent-ok > marker.txt` を実行し、"
                "その後 cat marker.txt で内容を確認して、内容をそのまま答えてください。",
            ),
            240,
        )
    finally:
        await engine.close()
    assert not any(isinstance(e, TurnFailed) for e in events), events
    started = [e for e in events if isinstance(e, ToolStarted)]
    finished = [e for e in events if isinstance(e, ToolFinished)]
    assert started and finished
    assert any(s.name == "bash" for s in started)
    assert all(f.status == "success" for f in finished)
    assert isinstance(events[-1], TurnFinished)
    assert "silk-agent-ok" in "".join(e.text for e in events if isinstance(e, AssistantText))
    assert (runtime.root("live") / "marker.txt").read_text().strip() == "silk-agent-ok"


async def test_interrupt_then_next_turn(setup):
    _, factory = setup
    engine = factory("live")
    events: list = []

    async def consume():
        async for e in engine.run_turn(
            TurnRequest("live", "t1", "bash ツールで `sleep 120` を実行してください。")
        ):
            events.append(e)

    task = asyncio.create_task(consume())
    try:
        for _ in range(240):
            if any(isinstance(e, ToolStarted) for e in events):
                break
            await asyncio.sleep(0.5)
        assert any(isinstance(e, ToolStarted) for e in events), events
        await engine.interrupt()
        await asyncio.wait_for(task, 60)
        assert not any(isinstance(e, TurnFinished) for e in events)
        # The same connection accepts a new instruction after the interrupt.
        events2 = await asyncio.wait_for(collect(engine, "1+1 は? 数字だけ答えて。", "t2"), 120)
        assert isinstance(events2[-1], TurnFinished), events2
        assert "2" in "".join(e.text for e in events2 if isinstance(e, AssistantText))
    finally:
        await engine.close()
