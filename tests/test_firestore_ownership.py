from datetime import UTC, datetime, timedelta

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.stores.firestore.ownership import ownership_matches


def test_ownership_matches_requires_active_unexpired_exact_epoch_and_generation():
    now = datetime(2026, 1, 1, tzinfo=UTC)
    token = OwnershipToken(
        "conversation",
        "worker",
        "incarnation",
        7,
        3,
        now + timedelta(seconds=60),
    )
    owner_data = {**token.to_dict(), "phase": "active"}
    conversation_data = {"conversation_generation": 3}

    assert ownership_matches(owner_data, conversation_data, token, now=now)
    assert not ownership_matches({**owner_data, "epoch": 6}, conversation_data, token, now=now)
    assert not ownership_matches(
        {**owner_data, "phase": "assigned"}, conversation_data, token, now=now
    )
    assert not ownership_matches(owner_data, {"conversation_generation": 4}, token, now=now)
    assert not ownership_matches(owner_data, conversation_data, token, now=token.expires_at)
