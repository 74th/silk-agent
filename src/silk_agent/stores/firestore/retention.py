"""Retention policy for Firestore records.

Every store computes ``expire_at`` here, so which states may expire is decided in one place.
A function returns ``None`` for a record that must be kept (processing, uncertain, held, or a
command whose completion notice has not been delivered). Firestore TTL deletes a record some
time after its ``expire_at`` (Terraform enables the policy per collection).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from silk_agent.config import RetentionConfig
from silk_agent.core.distributed_models import (
    CommandExecutionState,
    InputDeliveryState,
    NotificationState,
    OutputDeliveryState,
)

DEFAULT_TERMINAL_RETENTION = timedelta(days=30)
DEFAULT_RECEIPT_RETENTION = timedelta(days=7)
DEFAULT_WORKER_RETENTION = timedelta(days=7)

_TTL_ELIGIBLE_INPUT_STATES = frozenset(
    {
        InputDeliveryState.COMPLETED,
        InputDeliveryState.BLOCKED,
        InputDeliveryState.CANCELLED,
        InputDeliveryState.SUPPRESSED,
    }
)
_TTL_ELIGIBLE_CONTROL_STATES = frozenset({"handled", "filtered", "stale"})
_TTL_ELIGIBLE_OUTPUT_STATES = frozenset(
    {OutputDeliveryState.DELIVERED, OutputDeliveryState.SUPPRESSED}
)
_TERMINAL_COMMAND_STATES = frozenset(
    {
        CommandExecutionState.FINISHED,
        CommandExecutionState.CANCELLED,
        CommandExecutionState.UNKNOWN,
    }
)
# A command is kept while its completion notice may still reach the user.
_NOTIFIED_STATES = frozenset(
    {NotificationState.NONE, NotificationState.DELIVERED, NotificationState.SUPPRESSED}
)


@dataclass(frozen=True)
class RetentionPolicy:
    terminal: timedelta = DEFAULT_TERMINAL_RETENTION
    receipt: timedelta = DEFAULT_RECEIPT_RETENTION
    worker: timedelta = DEFAULT_WORKER_RETENTION

    def __post_init__(self) -> None:
        for name in ("terminal", "receipt", "worker"):
            if getattr(self, name) <= timedelta(0):
                raise ValueError(f"{name} retention must be positive")

    @classmethod
    def from_config(cls, config: RetentionConfig) -> RetentionPolicy:
        return cls(
            terminal=timedelta(days=config.terminal_days),
            receipt=timedelta(days=config.receipt_days),
            worker=timedelta(days=config.worker_days),
        )


def _expiry(at: datetime, field_name: str, retention: timedelta) -> datetime:
    if at.tzinfo is None or at.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    if at.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be UTC")
    if retention < timedelta(0):
        raise ValueError("retention must be non-negative")
    return at.astimezone(UTC) + retention


def terminal_expiry(
    state: InputDeliveryState,
    completed_at: datetime,
    *,
    retention: timedelta = DEFAULT_TERMINAL_RETENTION,
) -> datetime | None:
    """Return a TTL timestamp only for safely terminal inputs.

    Pending, dispatching, queued, accepted, held and uncertain records are deliberately
    retained until an explicit recovery or completion transition.
    """
    if state not in _TTL_ELIGIBLE_INPUT_STATES:
        return None
    return _expiry(completed_at, "completed_at", retention)


def control_expiry(
    authorization_state: str,
    at: datetime,
    *,
    retention: timedelta = DEFAULT_TERMINAL_RETENTION,
) -> datetime | None:
    """Controls (and ``control_queue``) expire once handled, filtered or stale."""
    if authorization_state not in _TTL_ELIGIBLE_CONTROL_STATES:
        return None
    return _expiry(at, "at", retention)


def output_expiry(
    state: OutputDeliveryState,
    at: datetime,
    *,
    retention: timedelta = DEFAULT_TERMINAL_RETENTION,
) -> datetime | None:
    """Outbox records expire once delivered or suppressed; an uncertain one stays."""
    if state not in _TTL_ELIGIBLE_OUTPUT_STATES:
        return None
    return _expiry(at, "at", retention)


def command_expiry(
    state: CommandExecutionState,
    notification_state: NotificationState,
    terminal_at: datetime,
    notified_at: datetime | None = None,
    *,
    retention: timedelta = DEFAULT_TERMINAL_RETENTION,
) -> datetime | None:
    """Commands (and their Sandbox operation) expire after the later of end and notice."""
    if state not in _TERMINAL_COMMAND_STATES or notification_state not in _NOTIFIED_STATES:
        return None
    start = max(terminal_at, notified_at) if notified_at is not None else terminal_at
    return _expiry(start, "terminal_at", retention)


def quiesce_expiry(
    state: str,
    answered_at: datetime,
    *,
    retention: timedelta = DEFAULT_TERMINAL_RETENTION,
) -> datetime | None:
    if state != "answered":
        return None
    return _expiry(answered_at, "answered_at", retention)


def receipt_expiry(
    received_at: datetime, *, retention: timedelta = DEFAULT_RECEIPT_RETENTION
) -> datetime:
    return _expiry(received_at, "received_at", retention)


def worker_expiry(
    heartbeat_at: datetime, *, retention: timedelta = DEFAULT_WORKER_RETENTION
) -> datetime:
    return _expiry(heartbeat_at, "heartbeat_at", retention)
