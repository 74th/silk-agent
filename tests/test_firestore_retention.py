from datetime import UTC, datetime, timedelta, timezone

import pytest

from silk_agent.config import RetentionConfig
from silk_agent.core.distributed_models import (
    CommandExecutionState,
    InputDeliveryState,
    NotificationState,
    OutputDeliveryState,
)
from silk_agent.stores.firestore.retention import (
    RetentionPolicy,
    command_expiry,
    control_expiry,
    output_expiry,
    quiesce_expiry,
    receipt_expiry,
    terminal_expiry,
    worker_expiry,
)

AT = datetime(2026, 1, 1, tzinfo=UTC)
DAY = timedelta(days=1)


@pytest.mark.parametrize(
    "state",
    [
        InputDeliveryState.PENDING,
        InputDeliveryState.DISPATCHING,
        InputDeliveryState.QUEUED,
        InputDeliveryState.ACCEPTED,
        InputDeliveryState.HELD,
        InputDeliveryState.UNCERTAIN,
    ],
)
def test_nonterminal_and_uncertain_inputs_have_no_ttl(state: InputDeliveryState) -> None:
    assert terminal_expiry(state, datetime(2026, 1, 1, tzinfo=UTC)) is None


@pytest.mark.parametrize(
    "state",
    [
        InputDeliveryState.COMPLETED,
        InputDeliveryState.BLOCKED,
        InputDeliveryState.CANCELLED,
        InputDeliveryState.SUPPRESSED,
    ],
)
def test_terminal_input_ttl_uses_configured_retention(state: InputDeliveryState) -> None:
    completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    assert terminal_expiry(state, completed_at) == completed_at + timedelta(days=30)


def test_ttl_requires_utc_timestamp_and_nonnegative_retention() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        terminal_expiry(InputDeliveryState.COMPLETED, datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="non-negative"):
        terminal_expiry(
            InputDeliveryState.COMPLETED,
            datetime(2026, 1, 1, tzinfo=UTC),
            retention=timedelta(days=-1),
        )


@pytest.mark.parametrize("state", ["handled", "filtered", "stale"])
def test_terminal_controls_expire(state: str) -> None:
    assert control_expiry(state, AT) == AT + 30 * DAY


@pytest.mark.parametrize("state", ["pending", "authorized"])
def test_unresolved_controls_have_no_ttl(state: str) -> None:
    assert control_expiry(state, AT) is None


@pytest.mark.parametrize(
    "state",
    [OutputDeliveryState.DELIVERED, OutputDeliveryState.SUPPRESSED, OutputDeliveryState.FAILED],
)
def test_settled_outputs_expire(state: OutputDeliveryState) -> None:
    assert output_expiry(state, AT) == AT + 30 * DAY


@pytest.mark.parametrize(
    "state",
    [OutputDeliveryState.PENDING, OutputDeliveryState.DISPATCHING, OutputDeliveryState.UNCERTAIN],
)
def test_unsettled_outputs_have_no_ttl(state: OutputDeliveryState) -> None:
    assert output_expiry(state, AT) is None


@pytest.mark.parametrize(
    "state",
    [
        CommandExecutionState.FINISHED,
        CommandExecutionState.CANCELLED,
        CommandExecutionState.UNKNOWN,
    ],
)
@pytest.mark.parametrize(
    "notification",
    [NotificationState.NONE, NotificationState.DELIVERED, NotificationState.SUPPRESSED],
)
def test_terminal_notified_commands_expire(
    state: CommandExecutionState, notification: NotificationState
) -> None:
    assert command_expiry(state, notification, AT) == AT + 30 * DAY


@pytest.mark.parametrize("notification", [NotificationState.PENDING, NotificationState.HELD])
def test_command_with_undelivered_notice_has_no_ttl(notification: NotificationState) -> None:
    assert command_expiry(CommandExecutionState.FINISHED, notification, AT) is None


@pytest.mark.parametrize(
    "state",
    [
        CommandExecutionState.INTENT,
        CommandExecutionState.RUNNING,
        CommandExecutionState.CANCEL_REQUESTED,
    ],
)
def test_running_command_has_no_ttl(state: CommandExecutionState) -> None:
    assert command_expiry(state, NotificationState.NONE, AT) is None


def test_command_expiry_counts_from_the_later_of_end_and_notice() -> None:
    notified = AT + 3 * DAY
    assert (
        command_expiry(CommandExecutionState.FINISHED, NotificationState.DELIVERED, AT, notified)
        == notified + 30 * DAY
    )
    assert (
        command_expiry(CommandExecutionState.FINISHED, NotificationState.DELIVERED, notified, AT)
        == notified + 30 * DAY
    )


def test_quiesce_expires_only_when_answered() -> None:
    assert quiesce_expiry("answered", AT) == AT + 30 * DAY
    assert quiesce_expiry("requested", AT) is None


def test_receipt_and_worker_use_their_own_retention() -> None:
    assert receipt_expiry(AT) == AT + 7 * DAY
    assert worker_expiry(AT) == AT + 7 * DAY
    assert receipt_expiry(AT, retention=3 * DAY) == AT + 3 * DAY


def test_policy_from_config_and_validation() -> None:
    policy = RetentionPolicy.from_config(
        RetentionConfig(terminal_days=14, receipt_days=3, worker_days=2)
    )
    assert (policy.terminal, policy.receipt, policy.worker) == (14 * DAY, 3 * DAY, 2 * DAY)
    with pytest.raises(ValueError, match="positive"):
        RetentionPolicy(terminal=timedelta(0))


def test_all_helpers_reject_naive_and_non_utc_times() -> None:
    naive = datetime(2026, 1, 1)
    jst = datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=9)))
    for bad in (naive, jst):
        with pytest.raises(ValueError):
            control_expiry("handled", bad)
        with pytest.raises(ValueError):
            output_expiry(OutputDeliveryState.DELIVERED, bad)
        with pytest.raises(ValueError):
            receipt_expiry(bad)
        with pytest.raises(ValueError):
            worker_expiry(bad)
