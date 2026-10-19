from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from silk_agent.core.distributed_models import CommandExecutionState, OwnershipToken
from silk_agent.core.ports import CommandResult
from silk_agent.sandbox.monitor import CommandMonitor
from silk_agent.sandbox.output_archive import CommandOutputArchiver, OutputPage
from tests.fakes.distributed_store import FakeSharedStore


class MutableClock:
    def __init__(self, now: datetime) -> None:
        self.value = now

    def now(self) -> datetime:
        return self.value


class StatusRuntime:
    def __init__(self, outcome: str = "running", *, block: bool = False) -> None:
        self.outcome = outcome
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        if not block:
            self.release.set()
        self.status_calls: list[tuple[str, str]] = []
        self.start_calls = 0

    async def command_status(self, conversation_key: str, command_id: str) -> CommandResult:
        self.status_calls.append((conversation_key, command_id))
        self.entered.set()
        await self.release.wait()
        return CommandResult(command_id, self.outcome)  # type: ignore[arg-type]


class OutputRuntime(StatusRuntime):
    async def read_output_page(
        self, _conversation_key, _command_id, *, stream, cursor, limit_bytes
    ) -> OutputPage:
        data = f"{stream} output".encode()
        page = data[cursor : cursor + limit_bytes]
        next_cursor = cursor + len(page)
        return OutputPage(page, next_cursor, next_cursor == len(data))


class MemoryOutputStore:
    async def put(
        self,
        *,
        command_id,
        sandbox_generation,
        stream,
        source,
        size_bytes,
        sha256,
        expires_at,
    ) -> str:
        return f"gs://test/{sandbox_generation}/{command_id}/{stream}"


class MemoryOutputReader:
    def __init__(self, values: dict[str, bytes]) -> None:
        self.values = values

    async def read(self, uri, *, cursor, limit_bytes, now):
        del now
        value = self.values[uri]
        data = value[cursor : cursor + limit_bytes]
        next_cursor = cursor + len(data)
        return data, next_cursor, next_cursor >= len(value)


async def test_monitor_b_takes_over_expired_lease_and_never_resends_start():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    clock = MutableClock(now)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    assert await store.create_intent(
        command_id="command-1",
        request_hash="request-hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=1,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )

    runtime_a = StatusRuntime("running", block=True)
    monitor_a = CommandMonitor(store, runtime_a, clock, monitor_id="monitor-a", lease_seconds=10)
    poll_a = asyncio.create_task(monitor_a.poll_once())
    await runtime_a.entered.wait()

    clock.value = now + timedelta(seconds=11)
    runtime_b = StatusRuntime("completed")
    monitor_b = CommandMonitor(store, runtime_b, clock, monitor_id="monitor-b", lease_seconds=10)
    assert await monitor_b.poll_once() == 1
    assert store.commands["command-1"]["state"] is CommandExecutionState.FINISHED

    runtime_a.release.set()
    await poll_a
    assert runtime_a.status_calls == [("conversation", "command-1")]
    assert runtime_b.status_calls == [("conversation", "command-1")]
    assert runtime_a.start_calls == runtime_b.start_calls == 0
    assert store.commands["command-1"]["state"] is CommandExecutionState.FINISHED


async def test_unchanged_status_does_not_write_command_state():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    clock = MutableClock(now)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="command-1",
        request_hash="request-hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=1,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    record = store.commands["command-1"]
    before = (record["state"], record.get("updated_at"), record["next_attempt_at"])
    runtime = StatusRuntime("unknown")
    monitor = CommandMonitor(store, runtime, clock, monitor_id="monitor-a")
    assert await monitor.poll_once() == 1
    after = (record["state"], record.get("updated_at"), record["next_attempt_at"])
    assert after == before


async def test_terminal_and_detach_race_selects_one_delivery_path():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="finished-before-detach",
        request_hash="hash-a",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=1,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    first = store.commands["finished-before-detach"]
    first["state"] = CommandExecutionState.RUNNING
    first_claim = await store.claim(
        "finished-before-detach",
        monitor_id="monitor",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert first_claim is not None
    terminal = await store.observe_terminal(
        "finished-before-detach",
        claim_token=first_claim["monitor_lease_token"],
        expected=CommandExecutionState.RUNNING,
        terminal=CommandExecutionState.FINISHED,
        result={"exit_code": 0},
        now=now,
    )
    detached = await store.detach("finished-before-detach", now=now)
    assert terminal is not None and terminal["decision"] == "return_result"
    assert detached is not None and detached["decision"] == "return_result"
    assert first["notification_state"] == "none"

    await store.create_intent(
        command_id="detached-before-finish",
        request_hash="hash-b",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=1,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    second = store.commands["detached-before-finish"]
    second["state"] = CommandExecutionState.RUNNING
    detached = await store.detach("detached-before-finish", now=now)
    second_claim = await store.claim(
        "detached-before-finish",
        monitor_id="monitor",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert detached is not None and detached["decision"] == "background"
    assert second_claim is not None
    terminal = await store.observe_terminal(
        "detached-before-finish",
        claim_token=second_claim["monitor_lease_token"],
        expected=CommandExecutionState.RUNNING,
        terminal=CommandExecutionState.FINISHED,
        result={"exit_code": 0},
        now=now,
    )
    assert terminal is not None and terminal["decision"] == "notify"
    assert second["notification_state"] == "pending"
    completion_id = "command-finished:1:detached-before-finish"
    assert ("conversation", completion_id) in store.inputs
    assert len([item for item in store.inputs if item[1] == completion_id]) == 1


async def test_orphaned_command_completion_becomes_system_notice():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="orphaned-command",
        request_hash="hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=1,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    record = store.commands["orphaned-command"]
    record["state"] = CommandExecutionState.RUNNING
    orphaned = await store.orphan("orphaned-command", now=now)
    claim = await store.claim(
        "orphaned-command",
        monitor_id="monitor",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert orphaned is not None and orphaned["decision"] == "orphaned"
    assert claim is not None
    terminal = await store.observe_terminal(
        "orphaned-command",
        claim_token=claim["monitor_lease_token"],
        expected=CommandExecutionState.RUNNING,
        terminal=CommandExecutionState.FINISHED,
        result={"exit_code": 0},
        now=now,
    )
    assert terminal is not None and terminal["decision"] == "notify"
    assert record["delivery_mode"] == "orphaned"
    assert record["notification_state"] == "pending"


async def test_cancel_request_is_durable_suppresses_notice_and_needs_confirmation():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="cancel-command",
        request_hash="hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=7,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    record = store.commands["cancel-command"]
    record["state"] = CommandExecutionState.RUNNING

    requested = await store.request_cancel("cancel-command", reason="user_stop", now=now)
    assert requested is not None
    request_id = requested["cancel_request_id"]
    assert requested["state"] is CommandExecutionState.CANCEL_REQUESTED
    assert requested["notification_suppressed"] is True

    assert await store.record_cancel_result(
        "cancel-command", request_id=request_id, outcome="unknown", now=now
    )
    assert record["state"] is CommandExecutionState.CANCEL_REQUESTED
    assert record["cancel_outcome"] == "unknown"

    claim = await store.claim(
        "cancel-command",
        monitor_id="monitor",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claim is not None
    terminal = await store.observe_terminal(
        "cancel-command",
        claim_token=claim["monitor_lease_token"],
        expected=CommandExecutionState.CANCEL_REQUESTED,
        terminal=CommandExecutionState.CANCELLED,
        result={"exit_code": None},
        now=now,
    )
    assert terminal is not None and terminal["decision"] == "suppressed"
    assert record["state"] is CommandExecutionState.CANCELLED
    assert record["cancel_confirmed_at"] == now
    assert record["cancel_outcome"] == "confirmed"
    assert record["notification_state"] == "suppressed"
    assert not any(item[1] == "command-finished:7:cancel-command" for item in store.inputs)


async def test_cancel_after_notice_is_persisted_suppresses_pending_input():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="late-cancel",
        request_hash="hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=3,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    record = store.commands["late-cancel"]
    record["state"] = CommandExecutionState.RUNNING
    await store.detach("late-cancel", now=now)
    claim = await store.claim(
        "late-cancel",
        monitor_id="monitor",
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claim is not None
    terminal = await store.observe_terminal(
        "late-cancel",
        claim_token=claim["monitor_lease_token"],
        expected=CommandExecutionState.RUNNING,
        terminal=CommandExecutionState.FINISHED,
        result={"exit_code": 0},
        now=now,
    )
    assert terminal is not None and terminal["decision"] == "notify"
    assert len(store.inputs) == 1

    cancelled = await store.request_cancel("late-cancel", reason="reset", now=now)
    assert cancelled is not None and cancelled["cancel_outcome"] == "too_late"
    assert record["state"] is CommandExecutionState.FINISHED
    assert record["notification_state"] == "suppressed"


async def test_monitor_persists_cancel_before_remote_request_and_keeps_unknown_uncertain():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    clock = MutableClock(now)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="cancel-unknown",
        request_hash="hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=1,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    store.commands["cancel-unknown"]["state"] = CommandExecutionState.RUNNING
    monitor = CommandMonitor(store, StatusRuntime(), clock, monitor_id="monitor")

    async def send_cancel(conversation_key: str, command_id: str, request_id: str) -> str:
        record = store.commands[command_id]
        assert record["state"] is CommandExecutionState.CANCEL_REQUESTED
        assert record["notification_suppressed"] is True
        assert record["cancel_request_id"] == request_id
        assert conversation_key == "conversation"
        return "unknown"

    response = await monitor.cancel("cancel-unknown", reason="user_stop", send_cancel=send_cancel)
    record = store.commands["cancel-unknown"]
    assert response is not None and response["cancel_outcome"] == "unknown"
    assert record["state"] is CommandExecutionState.CANCEL_REQUESTED
    assert record["cancel_outcome"] == "unknown"


async def test_monitor_archives_both_streams_before_terminal_commit():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    clock = MutableClock(now)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="archive-command",
        request_hash="hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=4,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    store.commands["archive-command"]["state"] = CommandExecutionState.RUNNING
    await store.detach("archive-command", now=now)
    runtime = OutputRuntime("completed")
    archiver = CommandOutputArchiver(MemoryOutputStore(), clock=clock, page_bytes=4)
    monitor = CommandMonitor(
        store,
        runtime,
        clock,
        monitor_id="monitor",
        output_archiver=archiver,
    )

    assert await monitor.poll_once() == 1
    record = store.commands["archive-command"]
    assert record["state"] is CommandExecutionState.FINISHED
    assert record["result"]["output"]["stdout"]["size_bytes"] == len(b"stdout output")
    assert record["result"]["output"]["stderr"]["size_bytes"] == len(b"stderr output")
    assert len(store.inputs) == 1


async def test_a_command_whose_sandbox_is_lost_is_ended_once_and_not_polled_again():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    clock = MutableClock(now)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="lost-command",
        request_hash="hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=1,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    store.commands["lost-command"]["state"] = CommandExecutionState.RUNNING
    await store.detach("lost-command", now=now)
    runtime = StatusRuntime("lost")
    monitor = CommandMonitor(store, runtime, clock, monitor_id="monitor")

    assert await monitor.poll_once() == 1
    record = store.commands["lost-command"]
    assert record["state"] is CommandExecutionState.UNKNOWN
    assert record["result"]["sandbox_lost"] is True
    assert len(store.inputs) == 1  # the conversation is told once
    clock.value += timedelta(minutes=5)
    assert await monitor.poll_once() == 0  # terminal: never read again
    assert len(runtime.status_calls) == 1


async def _running_command(store, now, command_id="stale-command", created_at=None):
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id=command_id,
        request_hash="hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=1,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=created_at or now,
    )
    store.commands[command_id]["state"] = CommandExecutionState.RUNNING
    await store.detach(command_id, now=now)


async def test_a_command_that_stays_unreadable_for_hours_is_ended_not_polled_for_ever():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    clock = MutableClock(now)
    store = FakeSharedStore()
    await _running_command(store, now)
    runtime = StatusRuntime("unknown")  # e.g. its Sandbox is idle-suspended
    monitor = CommandMonitor(store, runtime, clock, monitor_id="monitor", stale_after_seconds=3600)

    assert await monitor.poll_once() == 1  # claimed and read, still unknown
    assert store.commands["stale-command"]["state"] is CommandExecutionState.RUNNING
    clock.value += timedelta(hours=2)
    assert await monitor.poll_once() == 1
    record = store.commands["stale-command"]
    assert record["state"] is CommandExecutionState.UNKNOWN
    assert record["result"]["sandbox_lost"] is True
    clock.value += timedelta(minutes=5)
    assert await monitor.poll_once() == 0  # terminal: not read again


async def test_a_recent_command_with_an_unreadable_state_is_not_given_up_on():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    clock = MutableClock(now)
    store = FakeSharedStore()
    await _running_command(store, now)
    monitor = CommandMonitor(
        store, StatusRuntime("unknown"), clock, monitor_id="monitor", stale_after_seconds=3600
    )
    clock.value += timedelta(minutes=30)
    await monitor.poll_once()
    assert store.commands["stale-command"]["state"] is CommandExecutionState.RUNNING


async def test_monitor_reads_archived_output_with_a_combined_cursor():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    clock = MutableClock(now)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="archived-command",
        request_hash="hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=4,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    store.commands["archived-command"].update(
        state=CommandExecutionState.FINISHED,
        result={
            "exit_code": 0,
            "output": {
                "stdout": {"uri": "gs://test/stdout", "size_bytes": 3},
                "stderr": {"uri": "gs://test/stderr", "size_bytes": 2},
            },
        },
    )
    monitor = CommandMonitor(
        store,
        StatusRuntime(),
        clock,
        monitor_id="monitor",
        output_reader=MemoryOutputReader({"gs://test/stdout": b"abc", "gs://test/stderr": b"DE"}),
    )

    assert await monitor.command_output("archived-command", cursor=0, limit_bytes=2) == ("ab", 2)
    assert await monitor.command_output("archived-command", cursor=2, limit_bytes=2) == ("c", 3)
    assert await monitor.command_output("archived-command", cursor=3, limit_bytes=2) == ("DE", 5)


async def test_archived_output_with_an_empty_stdout_reads_stderr_from_the_start():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="stderr-only",
        request_hash="hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=4,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    store.commands["stderr-only"].update(
        state=CommandExecutionState.FINISHED,
        result={
            "exit_code": 1,
            "output": {
                "stdout": {"uri": "gs://test/stdout", "size_bytes": 0},
                "stderr": {"uri": "gs://test/stderr", "size_bytes": 3},
            },
        },
    )
    monitor = CommandMonitor(
        store,
        StatusRuntime(),
        MutableClock(now),
        monitor_id="monitor",
        output_reader=MemoryOutputReader({"gs://test/stdout": b"", "gs://test/stderr": b"err"}),
    )
    assert await monitor.command_output("stderr-only", cursor=0, limit_bytes=2) == ("er", 2)
    assert await monitor.command_output("stderr-only", cursor=2, limit_bytes=2) == ("r", 3)


async def test_monitor_does_not_finish_command_when_output_archive_fails():
    class FailingObjects:
        async def put(self, **_kwargs):
            raise OSError("upload unavailable")

    now = datetime(2026, 10, 14, tzinfo=UTC)
    clock = MutableClock(now)
    store = FakeSharedStore()
    owner = OwnershipToken(
        "conversation", "worker", "incarnation", 1, 1, now + timedelta(minutes=1)
    )
    store.owners["conversation"] = owner
    store.ownership_phases["conversation"] = "active"
    await store.create_intent(
        command_id="archive-failure",
        request_hash="hash",
        conversation_key="conversation",
        conversation_generation=1,
        sandbox_generation=4,
        origin_turn="turn-1",
        origin_epoch=1,
        origin_seq=1,
        owner=owner,
        created_at=now,
    )
    store.commands["archive-failure"]["state"] = CommandExecutionState.RUNNING
    runtime = OutputRuntime("completed")
    archiver = CommandOutputArchiver(FailingObjects(), clock=clock)
    monitor = CommandMonitor(
        store,
        runtime,
        clock,
        monitor_id="monitor",
        output_archiver=archiver,
    )

    assert await monitor.poll_once() == 1
    assert store.commands["archive-failure"]["state"] is CommandExecutionState.RUNNING
    assert store.inputs == {}
