"""Both execution paths hand the history store to the engine, which extends it before each turn.

The engine's own behavior (extend, existence check, notice) is covered in test_claude_engine;
this checks the wiring of the single-process path and of the distributed Worker.
"""

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import silk_agent.app as app_module
from silk_agent.config import Config
from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.distributed.worker_role import WorkerActorFactory
from silk_agent.testing.fakes import InMemoryRuntime, InMemoryStateStore, ScriptedChannel
from silk_agent.wiring import build_engine_factory
from tests.fakes.distributed_inbox import FakeInboxStore
from tests.fakes.distributed_store import FakeSharedStore
from tests.test_slack_bot import make_config

KEY = "T:C:thread"


class HistoryStore:
    def __init__(self):
        self.extended: list[tuple[str, str | None]] = []

    async def session_exists(self, session_id):
        return True

    async def extend_for_turn(self, conversation_key, session_id):
        self.extended.append((conversation_key, session_id))


async def extend_through(engine_factory, store, key="conv"):
    engine = engine_factory(key)
    assert engine._s.session_store is store
    notice, resume = await engine._prepare_history("old-session")
    assert notice is None and resume == "old-session"
    assert store.extended == [(key, "old-session")]


async def test_the_real_engine_factory_extends_the_history_it_is_given(clock, tmp_path):
    store = HistoryStore()
    factory = build_engine_factory(
        Config(), InMemoryRuntime(), clock, work_root=tmp_path, session_store=store
    )
    await extend_through(factory, store)


async def test_single_process_path_gives_the_engine_the_session_store(monkeypatch, clock, tmp_path):
    store = HistoryStore()
    captured = {}
    monkeypatch.setattr(
        app_module, "build_state_store", lambda config: (InMemoryStateStore(), store)
    )
    real = build_engine_factory

    def capturing(*args, **kwargs):
        captured.update(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(app_module, "build_engine_factory", capturing)
    services = await app_module.build_services(
        make_config(),
        ScriptedChannel(),
        work_dir=tmp_path,
        clock=clock,
        runtime=InMemoryRuntime(),
        start_background=False,
    )
    assert captured["session_store"] is store
    await services.aclose(grace_seconds=0)


async def test_worker_path_gives_the_engine_the_session_store(clock, tmp_path):
    store = HistoryStore()
    captured = {}
    real = build_engine_factory

    def builder(config, runtime, clk, **kwargs):
        captured.update(kwargs)
        return real(config, runtime, clk, **kwargs)

    shared = FakeSharedStore()
    inbox = FakeInboxStore()
    token = OwnershipToken(KEY, "w", "inc", 1, 1, clock.now() + timedelta(hours=1))
    inbox.owners[KEY] = token
    factory = WorkerActorFactory(
        config=Config(),
        clock=clock,
        inbox=inbox,
        conversations=SimpleNamespace(allocate_output_sequence=None),
        outbox_writer=shared,
        rate_limits=shared,
        command_monitor_store=shared,
        runtime=InMemoryRuntime(),
        session_store=store,
        skills=None,
        work_root=Path(tmp_path),
        monitor_id="w",
        build_engine_factory_fn=builder,
    )
    try:
        factory(token)
    except Exception:
        # Building the full actor needs more than this test provides; the engine factory
        # has been built by then.
        pass
    assert captured["session_store"] is store


@pytest.fixture
def clock():
    from silk_agent.testing.fakes import FakeClock

    return FakeClock()


async def test_conversation_resumed_without_its_history_continues_and_the_input_completes(
    clock, channel, tmp_path
):
    """The stored history is gone: the turn runs on a new SDK session, the user is told in the
    same conversation, and the durable input is completed (not failed or left uncertain)."""
    from silk_agent.agent.engine import HISTORY_LOST_NOTICE, ClaudeAgentEngine, ClaudeEngineSettings
    from silk_agent.core.events import Notice, TurnFailed, TurnFinished, UserMessage
    from silk_agent.core.ports import ConversationState
    from silk_agent.core.session import AgentSessionRegistry
    from silk_agent.distributed.input_delivery import DurableInputDelivery
    from tests.test_claude_engine import FakeSDKClient, result, text

    class GoneHistoryStore(HistoryStore):
        async def session_exists(self, session_id):
            return False

        def take_history_loss(self, session_id):
            return False

    inbox = FakeInboxStore()
    owner = OwnershipToken(KEY, "w", "inc", 1, 1, clock.now() + timedelta(hours=1))
    inbox.owners[KEY] = owner
    envelope = await inbox.accept_input(
        conversation_key=KEY,
        input_id="A",
        conversation_generation=1,
        kind="message",
        payload={"text": "続きをお願い"},
        created_at=clock.now(),
    )
    state_store = InMemoryStateStore()
    await state_store.put(ConversationState(KEY, agent_session_id="expired-session"))
    FakeSDKClient.instances = []
    settings = ClaudeEngineSettings(
        model="claude-haiku-5-5",
        project="p",
        region="global",
        work_root=Path(tmp_path),
        session_store=GoneHistoryStore(),
    )

    def client_factory(options):
        client = FakeSDKClient(options)
        client.script = [("msg", text("新しい履歴で答えます")), ("msg", result())]
        return client

    holder = {}

    async def completed(input_id):
        assert await holder["delivery"].complete(input_id)

    async def started(input_id):
        return await holder["delivery"].mark_started(input_id)

    registry = AgentSessionRegistry(
        lambda key: ClaudeAgentEngine(key, settings, clock, client_factory=client_factory),
        channel.send,
        clock,
        state_store=state_store,
        input_started=started,
        input_completed=completed,
    )
    session = await registry.get_or_create(KEY)
    holder["delivery"] = DurableInputDelivery(inbox, owner, session)
    assert await holder["delivery"].dispatch(envelope, UserMessage(KEY, "U", "続きをお願い")) == (
        "queued"
    )
    await channel.wait_for(TurnFinished)
    await registry.aclose()

    assert channel.of(TurnFailed) == []
    notice = channel.of(Notice, KEY)
    assert [n.text for n in notice] == [HISTORY_LOST_NOTICE]
    kinds = channel.kinds(KEY)
    assert kinds.index("Notice") < kinds.index("TurnFinished")  # same thread, before the answer
    options = FakeSDKClient.instances[0].options
    assert options.resume is None  # a new SDK session, not the expired one
    assert inbox.inputs[(KEY, "A")].delivery_state.value == "completed"
    saved = await state_store.get(KEY)
    assert saved is not None
    assert saved.history_reset_reason == "expired"
    assert saved.agent_session_id != "expired-session"
