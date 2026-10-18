from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.core.distributed_models import InputDeliveryState
from silk_agent.stores.firestore.retention import terminal_expiry


@pytest.mark.parametrize(
    "state",
    [
        InputDeliveryState.PENDING,
        InputDeliveryState.DISPATCHING,
        InputDeliveryState.ACCEPTED,
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
