from datetime import UTC, datetime

import pytest

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.distributed.recovery import DistributedRecoveryCoordinator


class Clock:
    def now(self):
        return datetime(2026, 10, 14, tzinfo=UTC)


class Ownership:
    def __init__(self):
        self.actions = []
        self.allow_evidence = True

    async def begin_recovery(self, token, *, now):
        self.actions.append(("begin", token, now))
        return True

    async def mark_recovery_blocked(self, token, *, reason, now):
        self.actions.append(("blocked", reason, now))
        return True

    async def confirm_stopped(self, token, *, evidence_id, worker_incarnation, now):
        self.actions.append(("confirm", evidence_id, worker_incarnation, now))
        return self.allow_evidence


@pytest.mark.asyncio
async def test_missing_quiesce_ack_keeps_old_ownership_blocked():
    store = Ownership()
    token = OwnershipToken(
        "conversation", "worker", "incarnation", 4, 2, datetime(2026, 10, 13, tzinfo=UTC)
    )

    async def unreachable(_token):
        raise TimeoutError

    result = await DistributedRecoveryCoordinator(store, Clock()).recover(
        token, request_quiesce=unreachable
    )
    assert result is not None and result.state == "blocked"
    assert [action[0] for action in store.actions] == ["begin", "blocked"]


@pytest.mark.asyncio
async def test_only_matching_incarnation_sdk_closed_ack_releases_ownership():
    store = Ownership()
    token = OwnershipToken(
        "conversation", "worker", "incarnation", 4, 2, datetime(2026, 10, 13, tzinfo=UTC)
    )

    async def quiesce(_token):
        return {
            "evidence_id": "close-ack-1",
            "worker_incarnation": "incarnation",
            "sdk_stopped": True,
        }

    result = await DistributedRecoveryCoordinator(store, Clock()).recover(
        token, request_quiesce=quiesce
    )
    assert result is not None and result.state == "released"
    assert result.evidence_id == "close-ack-1"
    assert store.actions[-1][0] == "confirm"
