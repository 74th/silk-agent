import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

import silk_agent.app as app_module
from silk_agent.config import Config
from silk_agent.core.events import UserMessage
from silk_agent.core.ports import ConversationState
from silk_agent.slack_app import SlackBot, check_config, load_tokens
from silk_agent.testing.fakes import (
    FakeAgentEngine,
    InMemoryRuntime,
    InMemoryStateStore,
    Text,
    Tool,
)
from tests.test_slack_output import FakeSlackClient

ROOT = Path(__file__).resolve().parent.parent
KEY = "slack:T1:C1:100.1"


class FakeSocket:
    def __init__(self, order):
        self.order = order

    async def start_async(self):
        self.order.append("socket-start")

    async def close_async(self):
        self.order.append("socket-close")


def make_config():
    cfg = Config()
    cfg.input_guard.provider = "allow_all"
    cfg.output_sanitizer.provider = "noop"
    cfg.session_store.provider = "memory"
    cfg.sandbox.runtime = "gke_session_sandbox"
    cfg.mcp.config_file = None
    cfg.filter.allowed_teams = []
    return cfg


def make_bot(monkeypatch, clock, tmp_path, store=None, scenario=None, order=None):
    store = store or InMemoryStateStore()
    order = order if order is not None else []
    client = FakeSlackClient()
    listeners = {}
    app = SimpleNamespace(client=client, event=lambda n: lambda f: listeners.setdefault(n, f) or f)
    monkeypatch.setattr(app_module, "build_state_store", lambda config: (store, None))
    engines = {}

    def fake_factory(*args, **kwargs):
        def factory(key):
            engines[key] = FakeAgentEngine(clock, scenario or [Text("ok")])
            return engines[key]

        return factory

    monkeypatch.setattr(app_module, "build_engine_factory", fake_factory)
    bot = SlackBot(
        make_config(),
        root=ROOT,
        work_dir=tmp_path,
        app=app,
        socket_handler=FakeSocket(order),
        clock=clock,
        bot_user_id="UBOT",
        shutdown_grace_seconds=25,
    )
    return bot, client, store, engines, order


def test_local_process_runtime_is_refused_for_slack():
    cfg = make_config()
    cfg.sandbox.runtime = "local_process"
    with pytest.raises(RuntimeError, match="local_process"):
        check_config(cfg)
    check_config(make_config())


def test_tokens_from_env_then_secret_manager(monkeypatch):
    cfg = make_config()
    cfg.slack.bot_token_secret = "projects/p/secrets/bot"
    cfg.slack.app_token_secret = "projects/p/secrets/app"
    assert load_tokens(cfg, {"SLACK_BOT_TOKEN": "xoxb-1", "SLACK_APP_TOKEN": "xapp-1"}) == (
        "xoxb-1",
        "xapp-1",
    )
    import silk_agent.slack_app as m

    monkeypatch.setattr(m, "read_secret", lambda name: name.rsplit("/", 1)[-1] + "-token")
    assert load_tokens(cfg, {}) == ("bot-token", "app-token")
    cfg.slack.bot_token_secret = ""
    with pytest.raises(RuntimeError):
        load_tokens(cfg, {"SLACK_APP_TOKEN": "x"})


async def test_start_recovers_turns_of_a_crashed_instance_and_does_not_resume_them(
    monkeypatch, clock, tmp_path
):
    store = InMemoryStateStore()
    await store.put(
        ConversationState(
            KEY,
            status="running",
            turn_id="t1",
            bot_instance_id="dead-bot",
            progress_ref={"channel": "C1", "ts": "150.0", "thread_ts": "100.1"},
        )
    )
    bot, client, store, engines, order = make_bot(monkeypatch, clock, tmp_path, store)
    client.history[("C1", "150.0")] = (
        "⏳ 実行中（経過 4分）　最終更新 2026-10-10 18:59\n⏳ 実行中 bash x"
    )
    await bot.start(runtime=InMemoryRuntime())
    assert bot.recovered == [KEY]
    assert client.updates[0]["ts"] == "150.0"
    assert client.updates[0]["text"].startswith(
        "⚠️ Botの停止により中断（最終更新 2026-10-10 18:59）"
    )
    assert "再開できます" in client.posts[-1]["text"]
    assert (await store.get(KEY)).status == "interrupted_crash"
    assert not engines  # nothing restarted automatically
    assert order == ["socket-start"]  # recovery ran before events were accepted


async def test_graceful_shutdown_order_and_interruption(monkeypatch, clock, tmp_path):
    bot, client, store, engines, order = make_bot(
        monkeypatch, clock, tmp_path, scenario=[Tool("bash", "sleep", duration=600)]
    )
    await bot.start(runtime=InMemoryRuntime())
    await bot._handle(UserMessage(KEY, "U1", "long job", team="T1", channel="C1"))
    await clock.advance(1)
    assert (await store.get(KEY)).status == "running"
    assert (await store.get(KEY)).progress_ref["channel"] == "C1"

    bot.request_stop()
    task = asyncio.create_task(bot.shutdown())
    await clock.advance(10)
    assert not task.done()  # inside the grace period the turn keeps running
    assert order == ["socket-start", "socket-close"]  # no new events are accepted any more
    await clock.advance(20)
    await asyncio.wait_for(task, 3)
    assert (await store.get(KEY)).status == "interrupted_shutdown"
    assert client.updates[-1]["text"].startswith("⚠️ Botの再起動により中断")
    assert engines[KEY].closed


async def test_user_email_is_resolved_for_the_sandbox_env(monkeypatch, clock, tmp_path):
    bot, client, *_ = make_bot(monkeypatch, clock, tmp_path)

    async def users_info(user):
        return {"user": {"profile": {"email": f"{user.lower()}@example.com"}}}

    client.users_info = users_info
    await bot.start(runtime=InMemoryRuntime())
    await bot._handle(UserMessage(KEY, "U1", "hi", team="T1", channel="C1"))
    assert await bot._env(KEY) == {"USER_EMAIL": "u1@example.com"}
    assert await bot._env("slack:T1:C9:1.0") == {}
