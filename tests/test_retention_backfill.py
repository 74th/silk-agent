from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.stores.firestore.retention import RetentionPolicy
from silk_agent.stores.firestore.retention_backfill import (
    ALREADY,
    KindReport,
    decide_command,
    decide_control,
    decide_input,
    decide_operation,
    decide_output,
    decide_quiesce,
    decide_rate_limit,
    decide_receipt,
    decide_worker,
    format_reports,
)

NOW = datetime(2026, 10, 18, 12, tzinfo=UTC)
DAY = timedelta(days=1)
POLICY = RetentionPolicy(terminal=30 * DAY, receipt=7 * DAY, worker=7 * DAY)


@pytest.mark.parametrize("state", ["completed", "blocked", "cancelled", "suppressed"])
def test_terminal_input_without_end_time_counts_from_now(state):
    decision = decide_input({"delivery_state": state}, NOW, POLICY)
    assert decision.expire_at == NOW + 30 * DAY


def test_terminal_input_with_a_stored_end_time_counts_from_it():
    ended = NOW - 5 * DAY
    assert (
        decide_input({"delivery_state": "completed", "completed_at": ended}, NOW, POLICY).expire_at
        == ended + 30 * DAY
    )


@pytest.mark.parametrize(
    "state", ["pending", "dispatching", "queued", "accepted", "held", "uncertain"]
)
def test_unfinished_inputs_are_left_alone(state):
    decision = decide_input({"delivery_state": state}, NOW, POLICY)
    assert decision.expire_at is None and decision.reason == f"state {state}"


def test_record_that_already_has_a_deadline_is_not_touched():
    for decide, data in (
        (decide_input, {"delivery_state": "completed"}),
        (decide_control, {"authorization_state": "handled"}),
        (decide_output, {"status": "delivered"}),
        (decide_command, {"state": "finished", "notification_state": "none"}),
        (decide_quiesce, {"state": "answered"}),
        (decide_receipt, {}),
        (decide_worker, {}),
        (decide_rate_limit, {}),
    ):
        decision = decide({**data, "expire_at": NOW + DAY}, NOW, POLICY)
        assert decision.expire_at is None and decision.reason == ALREADY, decide


def test_controls_and_outputs():
    assert decide_control({"authorization_state": "handled"}, NOW, POLICY).expire_at == (
        NOW + 30 * DAY
    )
    assert decide_control({"authorization_state": "authorized"}, NOW, POLICY).expire_at is None
    delivered_at = NOW - 3 * DAY
    assert (
        decide_output({"status": "delivered", "updated_at": delivered_at}, NOW, POLICY).expire_at
        == delivered_at + 30 * DAY
    )
    for state in ("pending", "dispatching", "uncertain"):
        assert decide_output({"status": state}, NOW, POLICY).expire_at is None


def test_commands_wait_for_their_notice():
    assert (
        decide_command({"state": "finished", "notification_state": "none"}, NOW, POLICY).expire_at
        == NOW + 30 * DAY
    )
    for notice in ("pending", "held"):
        decision = decide_command({"state": "finished", "notification_state": notice}, NOW, POLICY)
        assert decision.expire_at is None and decision.reason == f"notice {notice}"
    assert decide_command({"state": "running", "notification_state": "none"}, NOW, POLICY) == (
        decide_command({"state": "running"}, NOW, POLICY)
    )
    assert decide_command({"state": "running"}, NOW, POLICY).reason == "state running"


def test_command_counts_from_the_later_of_end_and_notice():
    end, notice = NOW - 10 * DAY, NOW - 2 * DAY
    decision = decide_command(
        {
            "state": "finished",
            "notification_state": "delivered",
            "completed_at": end,
            "notification_updated_at": notice,
        },
        NOW,
        POLICY,
    )
    assert decision.expire_at == notice + 30 * DAY


def test_operation_follows_its_command():
    command = {"expire_at": NOW + 3 * DAY}
    start = {"operation_kind": "command_start"}
    assert decide_operation(start, command, NOW, POLICY).expire_at == NOW + 3 * DAY
    assert decide_operation(start, None, NOW, POLICY).reason == "no command"
    assert decide_operation({"operation_kind": "file_write"}, command, NOW, POLICY).reason == (
        "no command"
    )
    assert decide_operation({**start, "expire_at": NOW}, command, NOW, POLICY).reason == ALREADY


def test_operation_counts_with_the_expiry_its_command_is_about_to_get():
    start = {"operation_kind": "command_start"}
    ended = {"state": "finished", "notification_state": "none"}
    assert decide_operation(start, ended, NOW, POLICY).expire_at == NOW + 30 * DAY
    for command in (
        {"state": "running", "notification_state": "none"},
        {"state": "finished", "notification_state": "pending"},
        {},
    ):
        assert decide_operation(start, command, NOW, POLICY).reason == "command not expiring"


def test_quiesce_receipt_and_worker():
    assert decide_quiesce({"state": "answered"}, NOW, POLICY).expire_at == NOW + 30 * DAY
    assert decide_quiesce({"state": "requested"}, NOW, POLICY).expire_at is None
    assert decide_receipt({}, NOW, POLICY).expire_at == NOW + 7 * DAY
    beat = NOW - 20 * DAY
    assert decide_worker({"heartbeat_at": beat.isoformat()}, NOW, POLICY).expire_at == (
        beat + 7 * DAY
    )


def test_rate_limit_record_expires_only_after_its_window_has_passed():
    active = NOW - timedelta(seconds=30)
    assert decide_rate_limit({"updated_at": active}, NOW, POLICY).reason == "window active"
    dormant = NOW - 20 * DAY
    assert decide_rate_limit({"updated_at": dormant}, NOW, POLICY).expire_at == dormant + DAY


def test_report_says_when_nothing_was_written():
    report = KindReport("inputs", examined=3, eligible=2, written=0)
    report.excluded["state held"] += 1
    text = format_reports([report], apply=False)
    assert "dry run" in text and "state held: 1" in text
    assert "applied" in format_reports([report], apply=True)
