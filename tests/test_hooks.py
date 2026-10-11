import asyncio
import json
import logging
from datetime import UTC, datetime

import pytest

from silk_agent.config import Config, HookEntry
from silk_agent.hooks import (
    BaseHooks,
    HookContext,
    HookDispatcher,
    HookLoadError,
    JsonLinesHooks,
    SystemNotice,
    ToolEnd,
    ToolStart,
    TurnStart,
    UserInput,
    json_lines,
    load_hooks,
)
from silk_agent.testing.fakes import RecordingHooks

AT = datetime(2026, 10, 10, 9, 0, tzinfo=UTC)
KEY = "slack:T1:C1:100.1"


def turn_start(i=0, key=KEY):
    return TurnStart(conversation_key=key, at=AT, turn_id=f"t{i}")


async def settle(n=200):
    for _ in range(n):
        await asyncio.sleep(0)


async def test_events_reach_every_hook_in_order():
    a, b = RecordingHooks(), RecordingHooks()
    d = HookDispatcher([a, b])
    for i in range(5):
        d.publish(turn_start(i))
    await settle()
    assert [e.turn_id for e in a.events] == [f"t{i}" for i in range(5)]
    assert [e.turn_id for e in b.events] == [f"t{i}" for i in range(5)]
    await d.aclose()


async def test_publish_does_not_wait_for_the_hooks():
    hooks = RecordingHooks()
    d = HookDispatcher([hooks])
    d.publish(turn_start())
    assert hooks.events == []  # delivered later by the worker task
    await settle()
    assert len(hooks.events) == 1
    await d.aclose()


async def test_common_fields_are_completed_from_the_last_input():
    hooks = RecordingHooks()
    d = HookDispatcher([hooks])
    d.publish(
        UserInput(
            conversation_key=KEY,
            at=AT,
            text="hi",
            decision="accepted",
            user="U1",
            user_email="a@example.com",
        )
    )
    d.publish(ToolStart(conversation_key=KEY, at=AT, tool_use_id="x", tool_name="Bash"))
    d.publish(turn_start(key="other"))
    await settle()
    tool = hooks.events[1]
    assert (tool.user, tool.user_email, tool.channel) == ("U1", "a@example.com", "slack")
    assert hooks.events[2].user is None
    await d.aclose()


async def test_an_exception_is_logged_and_the_other_hooks_still_get_the_event(caplog):
    class Failing(BaseHooks):
        async def on_tool_end(self, event):
            raise RuntimeError("boom")

    ok = RecordingHooks()
    d = HookDispatcher([Failing(), ok])
    with caplog.at_level(logging.ERROR):
        d.publish(
            ToolEnd(conversation_key=KEY, at=AT, tool_use_id="x", tool_name="n", status="success")
        )
        d.publish(turn_start())
        await settle()
    assert ok.kinds() == ["tool_end", "turn_start"]
    assert any("Failing" in r.getMessage() and r.exc_info for r in caplog.records)
    await d.aclose()


async def test_a_slow_hook_is_cut_off_after_the_timeout_and_does_not_block_the_caller(
    clock, caplog
):
    class Slow(BaseHooks):
        async def on_turn_start(self, event):
            await clock.sleep(10_000)

    ok = RecordingHooks()
    d = HookDispatcher([Slow(), ok], clock, hook_timeout_seconds=10)
    with caplog.at_level(logging.ERROR):
        d.publish(turn_start(1))
        d.publish(turn_start(2))
        await settle()
        assert ok.events == []  # the worker is inside the slow hook; the caller was not held up
        await clock.advance(10)
        await settle()
        assert [e.turn_id for e in ok.events] == ["t1"]
        await clock.advance(10)
        await settle()
    assert [e.turn_id for e in ok.events] == ["t1", "t2"]
    assert any("took more than" in r.getMessage() for r in caplog.records)
    await d.aclose()


async def test_a_full_queue_drops_the_oldest_events_and_logs_the_count(caplog):
    hooks = RecordingHooks()
    d = HookDispatcher([hooks], queue_size=3)
    with caplog.at_level(logging.WARNING):
        for i in range(6):
            d.publish(turn_start(i))  # the worker has not run yet: all of them queue up
        await settle()
    assert [e.turn_id for e in hooks.events] == ["t3", "t4", "t5"]
    assert d.dropped == 3
    assert any("dropped" in r.getMessage() for r in caplog.records)
    await d.aclose()


async def test_aclose_delivers_what_is_queued_and_closes_the_hooks():
    hooks = RecordingHooks()
    d = HookDispatcher([hooks])
    for i in range(3):
        d.publish(turn_start(i))
    await d.aclose()
    assert len(hooks.events) == 3 and hooks.closed
    d.publish(turn_start(9))  # after close: ignored
    await settle()
    assert len(hooks.events) == 3


async def test_aclose_gives_up_on_a_stuck_hook_after_the_limit(clock):
    class Stuck(RecordingHooks):
        async def on_event(self, event):
            await clock.sleep(10_000)

    stuck, ok = Stuck(), RecordingHooks()
    d = HookDispatcher([stuck, ok], clock, hook_timeout_seconds=10_000)
    d.publish(turn_start())
    await settle()
    closing = asyncio.ensure_future(d.aclose(timeout_seconds=5))
    await settle()
    assert not closing.done()
    await clock.advance(5)
    await asyncio.wait_for(closing, 1)
    assert stuck.closed and ok.closed


async def test_an_exception_in_aclose_is_logged(caplog):
    class Bad(RecordingHooks):
        async def aclose(self):
            raise RuntimeError("no")

    ok = RecordingHooks()
    d = HookDispatcher([Bad(), ok])
    with caplog.at_level(logging.ERROR):
        await d.aclose()
    assert ok.closed
    assert any("aclose" in r.getMessage() for r in caplog.records)


def test_base_hooks_dispatches_by_kind():
    seen = []

    class H(BaseHooks):
        async def on_system_notice(self, event):
            seen.append(event.text)

    asyncio.run(H().on_event(SystemNotice(conversation_key=KEY, at=AT, text="done")))
    asyncio.run(H().on_event(turn_start()))  # not overridden: ignored
    assert seen == ["done"]


# ---------------------------------------------------------------- loading


def config_with(*hooks):
    cfg = Config()
    cfg.hooks = list(hooks)
    return cfg


class _Factory:
    received = None


def make_hooks(ctx: HookContext):
    _Factory.received = ctx
    return RecordingHooks()


def test_a_hook_is_loaded_from_module_and_factory():
    (hook,) = load_hooks(config_with("tests.test_hooks:make_hooks"))
    assert isinstance(hook, RecordingHooks)
    assert dict(_Factory.received.options) == {}
    assert isinstance(_Factory.received.logger, logging.Logger)


def test_options_are_passed_to_the_factory():
    entry = HookEntry(factory="tests.test_hooks:make_hooks", options={"x": 1})
    load_hooks(config_with(entry))
    assert dict(_Factory.received.options) == {"x": 1}
    assert _Factory.received.config is not None


def test_a_missing_module_is_an_error_naming_the_hook():
    with pytest.raises(HookLoadError, match="no_such_pkg.hooks:create"):
        load_hooks(config_with("no_such_pkg.hooks:create"))


def test_a_missing_attribute_is_an_error_naming_the_hook():
    with pytest.raises(HookLoadError, match="tests.test_hooks:nope"):
        load_hooks(config_with("tests.test_hooks:nope"))


def test_a_name_without_a_factory_is_an_error():
    with pytest.raises(HookLoadError, match="package.module:factory"):
        load_hooks(config_with("tests.test_hooks"))


def test_a_factory_that_raises_is_an_error():
    with pytest.raises(HookLoadError, match="the factory failed"):
        load_hooks(config_with("tests.test_hooks:raising"))


def raising(ctx):
    raise RuntimeError("bad options")


def test_hooks_in_the_config_file_accept_strings_and_mappings(tmp_path):
    from silk_agent.config import load_config

    path = tmp_path / "c.yaml"
    path.write_text(
        "agent: {project: p}\ninput_guard: {provider: allow_all}\n"
        "output_sanitizer: {provider: noop}\nsession_store: {provider: memory}\n"
        "sandbox: {runtime: local_process}\n"
        "hooks:\n  - a.b:c\n"
        "  - {factory: silk_agent.hooks:json_lines, options: {include_content: true}}\n",
        encoding="utf-8",
    )
    cfg = load_config(path)
    assert cfg.hooks[0] == "a.b:c"
    assert cfg.hooks[1].options == {"include_content": True}
    assert cfg.hook_delivery.queue_size == 10000


# ---------------------------------------------------------------- JSON Lines hook


async def test_json_lines_writes_one_json_object_per_event():
    lines = []
    hook = JsonLinesHooks(write=lines.append)
    await hook.on_event(
        UserInput(
            conversation_key=KEY,
            at=AT,
            turn_id=None,
            text="secret text",
            decision="accepted",
            user="U1",
            channel="slack",
        )
    )
    await hook.on_event(turn_start())
    first, second = (json.loads(line) for line in lines)
    assert first["kind"] == "user_input" and first["conversation_key"] == KEY
    assert first["at"] == AT.isoformat() and first["decision"] == "accepted"
    assert "text" not in first  # content is off by default
    assert second["kind"] == "turn_start" and second["turn_id"] == "t0"
    assert all("\n" not in line for line in lines)


async def test_json_lines_content_is_included_on_request():
    lines = []
    hook = JsonLinesHooks(include_content=True, write=lines.append)
    await hook.on_event(
        ToolStart(
            conversation_key=KEY, at=AT, tool_use_id="x", tool_name="Bash", tool_input={"c": "ls"}
        )
    )
    assert json.loads(lines[0])["tool_input"] == {"c": "ls"}


def test_json_lines_factory_reads_the_option():
    ctx = HookContext({"include_content": True}, Config(), logging.getLogger("x"))
    assert json_lines(ctx)._include_content is True  # type: ignore[attr-defined]
    ctx = HookContext({}, Config(), logging.getLogger("x"))
    assert json_lines(ctx)._include_content is False  # type: ignore[attr-defined]


# ---------------------------------------------------------------- orchestrator events


def build(clock, channel, scenario, *, guard=None, filters=None, sanitizer=None, **kw):
    from silk_agent.core.orchestrator import ConversationOrchestrator
    from silk_agent.core.session import SessionSettings
    from silk_agent.core.stopwords import StopWords
    from silk_agent.testing.fakes import AllowAllGuard, FakeAgentEngine, NoopSanitizer

    hooks = RecordingHooks()
    dispatcher = HookDispatcher([hooks], clock)

    def factory(key):
        return FakeAgentEngine(clock, scenario, on_hook_event=dispatcher.publish)

    orch = ConversationOrchestrator(
        factory,
        channel,
        clock,
        guard=guard or AllowAllGuard(),
        sanitizer=sanitizer or NoopSanitizer(),
        stop_words=StopWords(["停止"]),
        filters=filters,
        settings=SessionSettings(),
        hooks=dispatcher,
        **kw,
    )
    return orch, dispatcher, hooks


def user_message(text, **kw):
    from silk_agent.core.events import SenderInfo, UserMessage

    kw.setdefault("sender", SenderInfo(email="a@example.com", is_guest=False, is_external=False))
    return UserMessage(KEY, "U1", text, **kw)


async def test_one_turn_is_reported_in_order(clock, channel):
    from silk_agent.core.events import TurnFinished
    from silk_agent.testing.fakes import Text, Tool

    orch, dispatcher, hooks = build(
        clock, channel, [Tool("bash", "ls", result="a.txt"), Text("done")]
    )
    await orch.handle(user_message("list files"))
    await channel.wait_for(TurnFinished)
    await dispatcher.aclose()
    assert hooks.kinds() == [
        "user_input",
        "turn_start",
        "tool_start",
        "tool_end",
        "agent_reply",
        "turn_end",
    ]
    user_input, _, tool_start, tool_end, reply, end = hooks.events
    assert (user_input.decision, user_input.text, user_input.user_email) == (
        "accepted",
        "list files",
        "a@example.com",
    )
    assert tool_start.tool_input == {"command": "ls"}
    assert tool_end.tool_response == "a.txt" and tool_end.status == "success"
    assert (reply.text, reply.sanitize_status) == ("done", "pass")
    assert end.outcome == "completed"
    # Everything after the input carries the sender.
    assert {e.user for e in hooks.events} == {"U1"}
    assert {e.turn_id for e in hooks.events[1:]} == {end.turn_id}


async def test_the_reply_is_reported_before_the_output_check_masks_it(clock, channel):
    from silk_agent.core.events import AssistantText, TurnFinished
    from silk_agent.testing.fakes import FakeSanitizer, Text

    orch, dispatcher, hooks = build(
        clock,
        channel,
        [Text("mail taro@example.com")],
        sanitizer=FakeSanitizer(["taro@example.com"]),
    )
    await orch.handle(user_message("who"))
    await channel.wait_for(TurnFinished)
    await dispatcher.aclose()
    (reply,) = [e for e in hooks.events if e.kind == "agent_reply"]
    assert reply.text == "mail taro@example.com" and reply.sanitize_status == "masked"
    assert "[MASKED]" in channel.of(AssistantText)[0].text


async def test_a_blocked_input_has_no_turn(clock, channel):
    from silk_agent.testing.fakes import RuleGuard, Text

    orch, dispatcher, hooks = build(clock, channel, [Text("x")], guard=RuleGuard(["secret"]))
    await orch.handle(user_message("the secret"))
    await dispatcher.aclose()
    (event,) = hooks.events
    assert event.kind == "user_input" and event.decision == "blocked"
    assert "secret" in event.reason


async def test_a_filtered_input_names_the_filter(clock, channel):
    from silk_agent.core.filters import default_chain
    from silk_agent.testing.fakes import Text

    orch, dispatcher, hooks = build(
        clock, channel, [Text("x")], filters=default_chain(clock, users=["U9"])
    )
    await orch.handle(user_message("hi"))
    await dispatcher.aclose()
    (event,) = hooks.events
    assert (event.decision, event.filter, event.user) == ("filtered", "AllowListFilter", "U1")


async def test_a_stop_word_is_reported(clock, channel):
    from silk_agent.testing.fakes import Text

    orch, dispatcher, hooks = build(clock, channel, [Text("x")])
    await orch.handle(user_message("停止"))
    await dispatcher.aclose()
    assert [(e.kind, e.decision) for e in hooks.events] == [("user_input", "stop_word")]


async def test_guard_failure_and_review_are_reported(clock, channel):
    from silk_agent.core.ports import GuardVerdict
    from silk_agent.testing.fakes import Text

    class Failing:
        async def check(self, text, history):
            raise RuntimeError("down")

    class Review:
        async def check(self, text, history):
            return GuardVerdict("review", "scope unclear")

    orch, dispatcher, hooks = build(clock, channel, [Text("x")], guard=Failing())
    await orch.handle(user_message("hi"))
    await dispatcher.aclose()
    assert hooks.events[0].decision == "guard_failed"

    orch, dispatcher, hooks = build(clock, channel, [Text("x")], guard=Review())
    await orch.handle(user_message("hi"))
    await dispatcher.aclose()
    assert (hooks.events[0].decision, hooks.events[0].reason) == ("review", "scope unclear")


async def test_a_failing_hook_does_not_stop_the_turn(clock, channel, caplog):
    from silk_agent.core.events import TurnFinished
    from silk_agent.core.orchestrator import ConversationOrchestrator
    from silk_agent.core.session import SessionSettings
    from silk_agent.core.stopwords import StopWords
    from silk_agent.testing.fakes import AllowAllGuard, FakeAgentEngine, NoopSanitizer, Text, Tool

    class Failing(BaseHooks):
        async def on_tool_end(self, event):
            raise RuntimeError("boom")

    dispatcher = HookDispatcher([Failing()], clock)
    orch = ConversationOrchestrator(
        lambda key: FakeAgentEngine(
            clock, [Tool("bash", "ls"), Text("done")], on_hook_event=dispatcher.publish
        ),
        channel,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords([]),
        settings=SessionSettings(),
        hooks=dispatcher,
    )
    with caplog.at_level(logging.ERROR):
        await orch.handle(user_message("go"))
        await channel.wait_for(TurnFinished)
        await dispatcher.aclose()
    assert any("Failing" in r.getMessage() for r in caplog.records)


async def test_a_slow_hook_does_not_delay_the_turn(clock, channel):
    from silk_agent.core.events import TurnFinished
    from silk_agent.core.orchestrator import ConversationOrchestrator
    from silk_agent.core.session import SessionSettings
    from silk_agent.core.stopwords import StopWords
    from silk_agent.testing.fakes import AllowAllGuard, FakeAgentEngine, NoopSanitizer, Text

    class Slow(BaseHooks):
        async def on_event(self, event):
            await clock.sleep(10)

    dispatcher = HookDispatcher([Slow()], clock, hook_timeout_seconds=60)
    orch = ConversationOrchestrator(
        lambda key: FakeAgentEngine(clock, [Text("done")], on_hook_event=dispatcher.publish),
        channel,
        clock,
        guard=AllowAllGuard(),
        sanitizer=NoopSanitizer(),
        stop_words=StopWords([]),
        settings=SessionSettings(),
        hooks=dispatcher,
    )
    await orch.handle(user_message("go"))
    await channel.wait_for(TurnFinished)  # no clock movement: not waiting for the hook
    assert clock.now() == AT.replace(hour=9)  # the fake clock was never advanced
    await dispatcher.aclose(timeout_seconds=0)


async def test_a_system_notice_is_reported(clock, channel):
    from silk_agent.core.events import TurnFinished
    from silk_agent.testing.fakes import Text

    orch, dispatcher, hooks = build(clock, channel, [Text("ok")])
    await orch.notify(KEY, "command abc finished")
    await channel.wait_for(TurnFinished)
    await dispatcher.aclose()
    assert hooks.events[0].kind == "system_notice"
    assert hooks.events[0].text == "command abc finished"
