"""Retention policy helpers for Firestore records."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from silk_agent.core.distributed_models import InputDeliveryState

DEFAULT_TERMINAL_RETENTION = timedelta(days=30)
_TTL_ELIGIBLE_INPUT_STATES = frozenset(
    {
        InputDeliveryState.COMPLETED,
        InputDeliveryState.BLOCKED,
        InputDeliveryState.CANCELLED,
        InputDeliveryState.SUPPRESSED,
    }
)


def terminal_expiry(
    state: InputDeliveryState,
    completed_at: datetime,
    *,
    retention: timedelta = DEFAULT_TERMINAL_RETENTION,
) -> datetime | None:
    """Return a TTL timestamp only for safely terminal inputs.

    Pending, dispatching, accepted, and uncertain records are deliberately
    retained until an explicit recovery or completion transition.
    """
    if state not in _TTL_ELIGIBLE_INPUT_STATES:
        return None
    if completed_at.tzinfo is None or completed_at.utcoffset() is None:
        raise ValueError("completed_at must be timezone-aware")
    if completed_at.utcoffset() != timedelta(0):
        raise ValueError("completed_at must be UTC")
    if retention < timedelta(0):
        raise ValueError("retention must be non-negative")
    return completed_at.astimezone(UTC) + retention
