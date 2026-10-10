from datetime import UTC, datetime

from silk_agent.channels.streamlit_channel import StreamlitChannel, build_view
from silk_agent.core.events import (
    GuardBlocked,
    Notice,
    ToolStarted,
    TurnFinished,
    TurnInterrupted,
    TurnStarted,
    UserMessage,
)
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.stopwords import StopWords
from silk_agent.testing.fakes import AllowAllGuard, FakeAgentEngine, NoopSanitizer, Text, Tool

K = "streamlit:abc"
NOW = datetime(2026, 10, 10, tzinfo=UTC)


def make(clock, scenario):
    ch = StreamlitChannel()
    orch = ConversationOrchestrator(
        lambda key: FakeAgentEngine(clock, scenario),
        ch,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(["停止"]),
    )
    return ch, orch


async def test_request_shows_answer_and_tools(clock):
    ch, orch = make(clock, [Tool("bash", "ls", duration=3), Text("done")])
    ch.add_user_message(K, "依頼")
    await orch.handle(UserMessage(K, "u", "依頼"))
    await clock.advance(3)
    for _ in range(50):
        if not ch.is_running(K):
            break
        await clock.advance(0)
    view = build_view(ch.log(K))
    assert [v.kind for v in view] == ["user", "tools", "assistant", "status"]
    tools = view[1]
    assert tools.state == "complete"
    assert [(t.name, t.summary, t.state, t.duration) for t in tools.tools] == [
        ("bash", "ls", "success", "3秒")
    ]
    assert view[2].text == "done" and view[3].text == "✅ 完了"


async def test_stop_word_interrupts_and_shows_it(clock):
    ch, orch = make(clock, [Tool("bash", "sleep", duration=100)])
    ch.add_user_message(K, "go")
    await orch.handle(UserMessage(K, "u", "go"))
    await clock.advance(1)
    assert ch.is_running(K)
    ch.add_user_message(K, "停止")
    await orch.handle(UserMessage(K, "u", "停止"))
    assert not ch.is_running(K)
    view = build_view(ch.log(K))
    assert [v.kind for v in view] == ["user", "tools", "user", "status"]
    assert view[1].state == "interrupted" and view[1].tools[0].state == "interrupted"
    assert view[3].text == "⏹ 中断しました"


def test_view_notices_blocks_and_open_tool_box():
    entries = [
        GuardBlocked(K, "input", "指示の上書きです"),
        Notice(K, "実行中の処理はありません。"),
        TurnStarted(K, "t", NOW),
        ToolStarted(K, "t", "1", "mcp", "x", NOW),
        TurnFinished(K, "t", warnings=("履歴が保存されていない",)),
    ]
    view = build_view(entries)
    assert view[0].text == "入力を処理しませんでした: 指示の上書きです"
    assert view[1].text == "実行中の処理はありません。"
    assert view[2].kind == "tools" and view[2].state == "complete"
    assert [v.text for v in view[3:]] == ["履歴が保存されていない", "✅ 完了"]


def test_is_running_tracks_terminal_events():
    ch = StreamlitChannel()
    assert not ch.is_running(K)
    ch._logs[K] = [TurnStarted(K, "t", NOW)]
    assert ch.is_running(K)
    ch._logs[K].append(TurnInterrupted(K, "t", "user", NOW))
    assert not ch.is_running(K)


async def test_ui_keeps_refreshing_while_the_guard_is_still_judging(clock):
    ch = StreamlitChannel()
    assert not ch.is_busy(K)
    ch.add_user_message(K, "依頼")  # sent; no event has answered it yet (Gemini is judging)
    assert not ch.is_running(K) and ch.is_busy(K)
    await ch.send(GuardBlocked(K, "input", "ブロック"))
    assert not ch.is_busy(K)
    ch.add_user_message(K, "silently dropped by a filter")
    assert ch.is_busy(K)
    assert not ch.is_busy(K, pending_seconds=0)  # gives up after the grace period
