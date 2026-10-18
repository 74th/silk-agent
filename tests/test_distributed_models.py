from datetime import UTC, datetime

import pytest

from silk_agent.core.distributed_models import (
    CommandDeliveryMode,
    CommandExecutionState,
    ControlEnvelope,
    InputDeliveryState,
    InputEnvelope,
    NotificationState,
    OutputDeliveryState,
    OwnershipToken,
    WorkerRecord,
    validate_state_transition,
)

NOW = datetime(2026, 10, 14, 3, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "record_type,record",
    [
        (
            InputEnvelope,
            InputEnvelope("team/C123/thread/456", 1, "input-1", 1, "message", {"text": "hi"}, NOW),
        ),
        (
            ControlEnvelope,
            ControlEnvelope(
                "team/C123/thread/456", 2, "control-1", 1, "stop", "U123", {"text": "stop"}, NOW
            ),
        ),
        (
            OwnershipToken,
            OwnershipToken("team/C123/thread/456", "worker-1", "pod-uid/start-uuid", 3, 1, NOW),
        ),
        (
            WorkerRecord,
            WorkerRecord("worker-1", "pod-uid/start-uuid", 4, 1, 1, False, NOW),
        ),
    ],
)
def test_distributed_record_serialization_round_trip(record_type, record):
    assert record_type.from_dict(record.to_dict()) == record


def test_input_envelope_rejects_unknown_delivery_state():
    data = InputEnvelope("conversation", 1, "input-1", 1, "message", {}, NOW).to_dict()
    data["delivery_state"] = "not-a-state"

    with pytest.raises(ValueError):
        InputEnvelope.from_dict(data)


@pytest.mark.parametrize("generation", [0, -1])
def test_ownership_token_rejects_invalid_conversation_generation(generation):
    with pytest.raises(ValueError, match="conversation_generation"):
        OwnershipToken("conversation", "worker", "incarnation", 1, generation, NOW)


def test_input_envelope_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        InputEnvelope("conversation", 1, "input-1", 1, "message", {}, datetime(2026, 10, 14))


def test_envelope_decode_rejects_naive_datetime():
    data = InputEnvelope("conversation", 1, "input-1", 1, "message", {}, NOW).to_dict()
    data["created_at"] = "2026-10-14T03:00:00"

    with pytest.raises(ValueError, match="timezone-aware"):
        InputEnvelope.from_dict(data)


def test_worker_record_rejects_usage_over_capacity():
    with pytest.raises(ValueError, match="cannot exceed capacity"):
        WorkerRecord("worker", "incarnation", 2, 3, 1, False, NOW)


def test_worker_record_accepts_legacy_document_without_drain_deadline():
    data = WorkerRecord("worker", "incarnation", 2, 0, 0, False, NOW).to_dict()
    data.pop("drain_deadline_at")
    assert WorkerRecord.from_dict(data).drain_deadline_at is None


def test_worker_record_validates_drain_deadline_timezone():
    with pytest.raises(ValueError, match="timezone-aware"):
        WorkerRecord("worker", "incarnation", 2, 0, 0, True, NOW, datetime(2026, 10, 14))


@pytest.mark.parametrize(
    "enum_type,expected",
    [
        (
            InputDeliveryState,
            {
                "pending",
                "dispatching",
                "queued",
                "accepted",
                "completed",
                "blocked",
                "cancelled",
                "suppressed",
                "held",
                "uncertain",
            },
        ),
        (
            CommandExecutionState,
            {"intent", "running", "finished", "cancel_requested", "cancelled", "unknown"},
        ),
        (NotificationState, {"none", "pending", "delivered", "suppressed", "held"}),
        (OutputDeliveryState, {"pending", "dispatching", "delivered", "suppressed", "uncertain"}),
        (CommandDeliveryMode, {"foreground", "background", "orphaned"}),
    ],
)
def test_states_have_distinct_persisted_values(enum_type, expected):
    assert {state.value for state in enum_type} == expected


def test_input_accepted_and_completed_are_distinct_states():
    assert InputDeliveryState.ACCEPTED is not InputDeliveryState.COMPLETED
    validate_state_transition(InputDeliveryState.DISPATCHING, InputDeliveryState.ACCEPTED)
    validate_state_transition(InputDeliveryState.ACCEPTED, InputDeliveryState.COMPLETED)


@pytest.mark.parametrize(
    "current,new",
    [
        (InputDeliveryState.COMPLETED, InputDeliveryState.PENDING),
        (CommandExecutionState.FINISHED, CommandExecutionState.RUNNING),
        (NotificationState.DELIVERED, NotificationState.PENDING),
        (OutputDeliveryState.DELIVERED, OutputDeliveryState.PENDING),
    ],
)
def test_terminal_states_cannot_be_reopened(current, new):
    with pytest.raises(ValueError, match="invalid"):
        validate_state_transition(current, new)


def test_notification_and_command_execution_states_are_independent():
    assert CommandExecutionState.RUNNING.value == "running"
    assert NotificationState.PENDING.value == "pending"
    assert "detach" not in {state.value for state in CommandExecutionState}
    assert CommandDeliveryMode.BACKGROUND.value == "background"
    assert "cancel" not in {state.value for state in NotificationState}
