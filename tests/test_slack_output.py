import re
from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.channels.slack import SlackChannel
from silk_agent.channels.slack_output import SlackOutput, rewrite_as_crashed
from silk_agent.channels.slack_progress import ProgressView, format_duration
from silk_agent.config import ProgressConfig
from silk_agent.core.events import (
    AssistantText,
    GuardBlocked,
    Heartbeat,
    MessageAppended,
    Notice,
    StatusChanged,
    ToolFinished,
    ToolStarted,
    TurnFinished,
    TurnInterrupted,
    TurnStarted,
    Usage,
    UserMessage,
)
from silk_agent.core.orchestrator import ConversationOrchestrator
from silk_agent.core.ports import ConversationState
from silk_agent.core.session import SessionSettings
from silk_agent.core.stopwords import StopWords
from silk_agent.testing.fakes import (
    AllowAllGuard,
    FakeAgentEngine,
    InMemoryStateStore,
    NoopSanitizer,
    Text,
    Tool,
)

KEY = "slack:T1:C1:100.1"
T0 = datetime(2026, 10, 10, 9, 59, 30, tzinfo=UTC)  # 18:59:30 JST


class FakeSlackClient:
    def __init__(self):
        self.posts: list[dict] = []
        self.updates: list[dict] = []
        self.uploads: list[dict] = []
        self.history: dict[tuple[str, str], str] = {}
        self.fail_update = False
        self._n = 0

    async def chat_postMessage(self, **kw):
        self._n += 1
        self.posts.append(kw)
        return {"ts": f"{200 + self._n}.0"}

    async def chat_update(self, **kw):
        if self.fail_update:
            raise RuntimeError("message_not_found")
        self.updates.append(kw)
        return {"ok": True}

    async def files_upload_v2(self, **kw):
        self.uploads.append(kw)
        return {"ok": True}

    async def conversations_history(self, channel, latest, inclusive, limit):
        text = self.history.get((channel, latest))
        return {"messages": [{"text": text}] if text is not None else []}


def tool_started(i, name="bash", summary="uv run pytest", at=T0):
    return ToolStarted(KEY, "t1", f"tu{i}", name, summary, at)


# ---------------------------------------------------------------- ProgressView


def test_view_rendering_running_and_tools():
    v = ProgressView(T0)
    v.apply(tool_started(1, "bash", "git clone https://x/y"))
    v.apply(ToolFinished(KEY, "t1", "tu1", "success", "git clone https://x/y", T0, 3))
    v.apply(tool_started(2, "docs/search", "query=silk"))
    text = v.render(T0 + timedelta(minutes=4))
    lines = text.splitlines()
    assert lines[0] == "⏳ 実行中（経過 4分）　最終更新 2026-10-10 19:03"
    assert "✅ 成功 bash git clone https://x/y（3秒）" in lines
    assert "⏳ 実行中 docs/search query=silk" in lines
    assert lines[-1] == "停止するには『停止』と書き込んでください"


def test_view_shows_appended_messages_while_running():
    v = ProgressView(T0)
    v.apply(tool_started(1, "bash", "sleep 300 && echo done"))
    v.apply(MessageAppended(KEY, "t1"))
    v.apply(MessageAppended(KEY, "t1"))
    lines = v.render(T0).splitlines()
    assert "📝 追加の指示 2件（実行中のツールが終わったら、この処理の続きで対応します）" in lines
    v.apply(ToolFinished(KEY, "t1", "tu1", "success", "sleep 300 && echo done", T0, 300))
    v.apply(TurnFinished(KEY, "t1", "", Usage(), finished_at=T0))
    assert not any(line.startswith("📝") for line in v.render(T0).splitlines())


def test_view_shows_sandbox_start_up_until_the_agent_runs():
    v = ProgressView(T0)
    v.apply(StatusChanged(KEY, "t1", "preparing_sandbox"))
    assert v.render(T0).splitlines()[0].startswith("🛠 作業環境を起動しています…")
    v.apply(StatusChanged(KEY, "t1", "thinking"))
    assert v.render(T0).splitlines()[0].startswith("⏳ 実行中")


def test_view_shows_latest_20_and_remaining_count():
    v = ProgressView(T0)
    for i in range(25):
        v.apply(tool_started(i, "bash", f"cmd{i}"))
        v.apply(ToolFinished(KEY, "t1", f"tu{i}", "success", f"cmd{i}", T0, 1))
    lines = v.render(T0).splitlines()
    assert "…ほか5件" in lines
    tool_lines = [line for line in lines if line.startswith("✅")]
    assert len(tool_lines) == 20 and "cmd24" in tool_lines[-1] and "cmd5" in tool_lines[0]


def test_view_completed_shows_duration_tool_count_and_stamp():
    v = ProgressView(T0)
    v.apply(tool_started(1))
    v.apply(ToolFinished(KEY, "t1", "tu1", "success", "", T0, 2))
    end = T0 + timedelta(minutes=6)
    v.apply(TurnFinished(KEY, "t1", "", Usage(cost_usd=0.42), ("履歴が保存されていない",), end))
    text = v.render(end + timedelta(minutes=10))
    assert (
        text.splitlines()[0]
        == "✅ 完了（所要 6分 / ツール 1回 / $0.42）　最終更新 2026-10-10 19:15"
    )
    assert "⚠️ 履歴が保存されていない" in text
    assert "停止するには" not in text


def test_view_interrupted_variants():
    for reason, header in [
        ("user", "⏹ 中断しました"),
        ("new_message", "↪ 新しい指示で再開"),
        ("shutdown", "⚠️ Botの再起動により中断"),
        ("crash", "⚠️ Botの停止により中断"),
    ]:
        v = ProgressView(T0)
        v.apply(tool_started(1))
        v.apply(TurnInterrupted(KEY, "t1", reason, T0))
        text = v.render(T0)
        assert text.startswith(header) and "⏹ 中断 bash" in text
    assert "「続けて」" in text


def test_interrupted_turn_keeps_background_command_visible():
    v = ProgressView(T0)
    v.apply(tool_started(1, summary="make build"))
    v.apply(ToolFinished(KEY, "t1", "tu1", "background", "make build", T0, 20))
    v.apply(TurnInterrupted(KEY, "t1", "drain", T0))

    text = v.render(T0).splitlines()

    assert text[0].startswith("⚠️ Workerの縮退により中断")
    assert "🔄 バックグラウンドで実行中 bash make build（20秒）" in text
    assert any("「続けて」" in line for line in text)


def test_tool_display_name_only_and_duration_format():
    v = ProgressView(T0, tool_display="name")
    v.apply(tool_started(1, "bash", "secret-ish summary"))
    assert "secret-ish" not in v.render(T0)
    assert [format_duration(x) for x in (0, 59, 60, 65, 3600, 3660)] == [
        "0秒",
        "59秒",
        "1分",
        "1分5秒",
        "1時間",
        "1時間1分",
    ]


# ---------------------------------------------------------------- SlackOutput


def make_output(clock, client=None, store=None, **kw):
    client = client or FakeSlackClient()
    return SlackOutput(client, clock, ProgressConfig(), state_store=store, **kw), client


async def test_turn_start_posts_progress_message_in_thread(clock):
    out, client = make_output(clock)
    await out.send(TurnStarted(KEY, "t1", clock.now()))
    (post,) = client.posts
    assert post["channel"] == "C1" and post["thread_ts"] == "100.1"
    assert post["text"].startswith("⏳ 実行中") and "最終更新 2026-10-10 18:00" in post["text"]
    assert "停止するには『停止』と書き込んでください" in post["text"]


async def test_a_rejected_turn_start_is_posted_again_when_the_event_is_retried(clock):
    out, client = make_output(clock)
    real_post = client.chat_postMessage
    failures = [RuntimeError("service_unavailable")]

    async def flaky(**kw):
        if failures:
            raise failures.pop()
        return await real_post(**kw)

    client.chat_postMessage = flaky
    event = TurnStarted(KEY, "t1", clock.now())
    with pytest.raises(RuntimeError):
        await out.send_durable(event)
    assert client.posts == []
    # The Outbox sends the same event again: it must reach Slack this time, not be skipped
    # as a turn that is already on screen.
    await out.send_durable(event)
    assert len(client.posts) == 1
    await out.send_durable(event)  # a genuine repeat of a posted turn is still ignored
    assert len(client.posts) == 1


async def test_updates_are_throttled_to_one_per_second(clock):
    out, client = make_output(clock)
    await out.send(TurnStarted(KEY, "t1", clock.now()))
    await clock.advance(2)
    for i in range(10):  # a burst of tool events inside one second
        await out.send(tool_started(i, summary=f"c{i}", at=clock.now()))
    assert len(client.updates) == 1  # the first goes out at once, the rest are batched
    await clock.advance(1)
    assert len(client.updates) == 2
    assert "c9" in client.updates[-1]["text"]  # the batch carries the newest state


async def test_last_update_advances_every_minute_during_a_5_minute_tool():
    from silk_agent.testing.fakes import FakeClock

    clock = FakeClock(datetime(2026, 10, 10, 9, 0, 0, tzinfo=UTC))
    client, channel_store = FakeSlackClient(), InMemoryStateStore()
    holder = {}

    async def handle(e):
        await holder["orch"].handle(e)

    # Slack channel + orchestrator + fake agent: the real wiring, no network
    from types import SimpleNamespace

    app = SimpleNamespace(client=client, event=lambda name: lambda f: f)
    channel = SlackChannel(app, handle, clock, ProgressConfig(), state_store=channel_store)
    orch = ConversationOrchestrator(
        lambda key: FakeAgentEngine(clock, [Tool("bash", "sleep 300", duration=300), Text("done")]),
        channel,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords(["停止"]),
        settings=SessionSettings(heartbeat_interval_seconds=60),
        state_store=channel_store,
    )
    holder["orch"] = orch
    await orch.handle(UserMessage(KEY, "U1", "run it", team="T1", channel="C1"))
    await clock.advance(1)
    for _ in range(5):
        await clock.advance(60)
    stamps = [
        m.group(1)
        for t in [client.posts[0]["text"], *[u["text"] for u in client.updates]]
        if (m := re.search(r"最終更新 (\S+ \S+)", t))
    ]
    hhmm = []
    for s in stamps:
        if not hhmm or hhmm[-1] != s:
            hhmm.append(s)
    assert [s[-5:] for s in hhmm] == ["18:00", "18:01", "18:02", "18:03", "18:04", "18:05"]
    assert client.updates[-1]["text"].splitlines()[0].startswith("✅ 完了")


async def test_finish_flushes_immediately(clock):
    out, client = make_output(clock)
    await out.send(TurnStarted(KEY, "t1", clock.now()))
    await out.send(TurnFinished(KEY, "t1", "", Usage(), (), clock.now()))
    assert client.updates[-1]["text"].startswith("✅ 完了")


async def test_failed_update_reposts_progress_message(clock):
    store = InMemoryStateStore()
    out, client = make_output(clock, store=store)
    await out.send(TurnStarted(KEY, "t1", clock.now()))
    first_ts = (await store.get(KEY)).progress_ref["ts"]
    client.fail_update = True
    await clock.advance(2)
    await out.send(tool_started(1, at=clock.now()))
    assert len(client.posts) == 2  # reposted
    assert "bash" in client.posts[1]["text"]
    assert (await store.get(KEY)).progress_ref["ts"] != first_ts


async def test_progress_position_is_saved_in_state(clock):
    store = InMemoryStateStore()
    out, client = make_output(clock, store=store)
    await out.send(TurnStarted(KEY, "t1", clock.now()))
    ref = (await store.get(KEY)).progress_ref
    assert ref == {"channel": "C1", "ts": "201.0", "thread_ts": "100.1"}


async def test_sender_restart_restores_progress_view_and_slack_reference(clock):
    store = InMemoryStateStore()
    client = FakeSlackClient()
    first, _ = make_output(clock, client, store)
    await first.send(TurnStarted(KEY, "t1", clock.now()))
    first_ref = (await store.get(KEY)).progress_ref["ts"]
    await first.send(tool_started(1, at=clock.now()))
    saved = await store.get(KEY)
    assert saved.progress_snapshot["turn_id"] == "t1"
    assert saved.progress_snapshot["view"]["tools"][0]["name"] == "bash"

    restarted, _ = make_output(clock, client, store)
    await restarted.send(tool_started(2, summary="restored", at=clock.now()))
    assert len(client.posts) == 1
    assert client.updates[-1]["ts"] == first_ref
    assert "restored" in client.updates[-1]["text"]


async def test_heartbeat_after_terminal_does_not_reopen_progress(clock):
    out, client = make_output(clock, store=InMemoryStateStore())
    await out.send(TurnStarted(KEY, "t1", clock.now()))
    await out.send(TurnFinished(KEY, "t1", "", Usage(), finished_at=clock.now()))
    updates = len(client.updates)
    await out.send(Heartbeat(KEY, "t1", clock.now()))
    assert len(client.updates) == updates
    assert client.updates[-1]["text"].startswith("✅ 完了")


async def test_replies_notices_and_blocks(clock):
    out, client = make_output(clock)
    await out.send(AssistantText(KEY, "t1", "こんにちは"))
    await out.send(Notice(KEY, "実行中の処理はありません。"))
    await out.send(GuardBlocked(KEY, "input", "指示の上書きです"))
    await out.send(GuardBlocked(KEY, "output", "ポリシーにより表示できません"))
    assert [p["text"] for p in client.posts] == [
        "こんにちは",
        "実行中の処理はありません。",
        "⚠️ 入力を処理しませんでした: 指示の上書きです",
        "⚠️ ポリシーにより表示できません",
    ]
    assert all(p["thread_ts"] == "100.1" for p in client.posts)


async def test_long_reply_is_uploaded_as_file(clock):
    out, client = make_output(clock)
    await out.send(AssistantText(KEY, "t1", "x" * 12_001))
    assert not client.posts
    (up,) = client.uploads
    assert up["channel"] == "C1" and up["thread_ts"] == "100.1" and len(up["content"]) == 12_001
    await out.send(AssistantText(KEY, "t1", "x" * 12_000))
    assert len(client.posts) == 1  # exactly at the limit stays a message


async def test_dm_replies_have_no_thread(clock):
    out, client = make_output(clock)
    await out.send(AssistantText("slack:T1:D1:-", "t1", "hi"))
    assert "thread_ts" not in client.posts[0]


async def test_shutdown_interruption_is_shown_and_announced(clock):
    out, client = make_output(clock)
    await out.send(TurnStarted(KEY, "t1", clock.now()))
    await out.send(tool_started(1, at=clock.now()))
    await out.send(TurnInterrupted(KEY, "t1", "shutdown", clock.now()))
    assert client.updates[-1]["text"].startswith("⚠️ Botの再起動により中断")
    assert "再開できます" in client.posts[-1]["text"]


# ---------------------------------------------------------------- after a restart


async def test_crash_rewrites_old_progress_message_with_last_update_time(clock):
    store = InMemoryStateStore()
    await store.put(
        ConversationState(
            KEY,
            status="interrupted_crash",
            progress_ref={"channel": "C1", "ts": "150.0", "thread_ts": "100.1"},
        )
    )
    client = FakeSlackClient()
    client.history[("C1", "150.0")] = (
        "⏳ 実行中（経過 4分）　最終更新 2026-10-10 18:59\n"
        "✅ 成功 bash git clone（3秒）\n⏳ 実行中 bash uv run pytest\n"
        "停止するには『停止』と書き込んでください"
    )
    out, _ = make_output(clock, client, store)
    await out.send(TurnInterrupted(KEY, "t1", "crash", clock.now()))
    (update,) = client.updates
    assert update["ts"] == "150.0"
    lines = update["text"].splitlines()
    assert lines[0] == "⚠️ Botの停止により中断（最終更新 2026-10-10 18:59）"
    assert "✅ 成功 bash git clone（3秒）" in lines and "⏹ 中断 bash uv run pytest" in lines
    assert not any("停止するには" in line for line in lines)
    assert "「続けて」と書き込むと再開できます" in client.posts[-1]["text"]
    assert client.posts[-1]["thread_ts"] == "100.1"


async def test_crash_without_readable_old_message_still_notifies(clock):
    store = InMemoryStateStore()
    await store.put(ConversationState(KEY, progress_ref={"channel": "C1", "ts": "150.0"}))
    out, client = make_output(clock, FakeSlackClient(), store)
    await out.send(TurnInterrupted(KEY, "t1", "crash", clock.now()))
    assert client.updates[0]["text"].startswith(
        "⚠️ Botの停止により中断（最終更新 2026-10-10 18:00）"
    )
    assert client.posts


def test_rewrite_as_crashed_uses_fallback_stamp_when_none_found():
    assert rewrite_as_crashed("", "2026-01-01 00:00").startswith(
        "⚠️ Botの停止により中断（最終更新 2026-01-01 00:00）"
    )


async def test_ordering_with_heartbeat_event_does_not_crash(clock):
    out, client = make_output(clock)
    await out.send(Heartbeat(KEY, "t1", clock.now()))  # no view yet: ignored
    await out.send(StatusChanged(KEY, "t1", "interrupting"))
    assert not client.posts and not client.updates


async def test_bolt_listeners_are_registered_and_dispatch_without_blocking(clock):
    from types import SimpleNamespace

    listeners = {}

    def event(name):
        def deco(fn):
            listeners[name] = fn
            return fn

        return deco

    handled = []

    async def handle(e):
        handled.append(e)

    app = SimpleNamespace(client=FakeSlackClient(), event=event)
    channel = SlackChannel(app, handle, clock, ProgressConfig(), bot_user_id="UBOT")
    channel.register()
    assert set(listeners) == {"app_mention", "message"}
    await listeners["app_mention"](
        {
            "team_id": "T1",
            "event_id": "E1",
            "event": {"user": "U1", "text": "<@UBOT> hi", "channel": "C1", "ts": "1.0"},
        }
    )
    await channel.wait_idle()
    assert handled[0].text == "hi" and handled[0].conversation_key == "slack:T1:C1:1.0"
    # the thread the mention started now counts as one the Bot takes part in
    await listeners["message"](
        {
            "team_id": "T1",
            "event_id": "E2",
            "event": {
                "user": "U1",
                "text": "more",
                "channel": "C1",
                "channel_type": "channel",
                "ts": "2.0",
                "thread_ts": "1.0",
            },
        }
    )
    await channel.wait_idle()
    assert [h.text for h in handled] == ["hi", "more"]


async def test_participation_survives_restart_through_state_store(clock):
    from types import SimpleNamespace

    store = InMemoryStateStore()
    await store.put(ConversationState("slack:T1:C1:1.0"))
    handled = []

    async def handle(e):
        handled.append(e)

    app = SimpleNamespace(client=FakeSlackClient(), event=lambda n: lambda f: f)
    channel = SlackChannel(
        app, handle, clock, ProgressConfig(), state_store=store, bot_user_id="UBOT"
    )
    await channel.router.on_message(
        {
            "team_id": "T1",
            "event_id": "E9",
            "event": {
                "user": "U1",
                "text": "続けて",
                "channel": "C1",
                "channel_type": "channel",
                "ts": "3.0",
                "thread_ts": "1.0",
            },
        }
    )
    assert handled and handled[0].text == "続けて"


async def test_wait_idle_gives_up_after_the_grace_period():
    import asyncio
    from types import SimpleNamespace

    app = SimpleNamespace(client=FakeSlackClient(), event=lambda *a, **k: None)
    channel = SlackChannel(app, None, None, ProgressConfig())
    blocked = asyncio.create_task(asyncio.Event().wait())
    channel._tasks.add(blocked)
    try:
        await asyncio.wait_for(channel.wait_idle(0.05), 2)
        await asyncio.sleep(0)
        assert not blocked.cancelled()  # giving up the wait leaves the handler running
    finally:
        blocked.cancel()
