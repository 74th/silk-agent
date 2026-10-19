from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.distributed.control_handler import WorkerControlHandler

NOW = datetime(2026, 10, 19, tzinfo=UTC)
OWNER = OwnershipToken("slack:T:C:1", "worker-a", "inc", 1, 1, NOW)


class Inbox:
    def __init__(self):
        self.closed = []

    async def mark_control_handled(self, *, conversation_key, input_id, owner, now):
        self.closed.append(input_id)
        return True


def handler(inbox):
    return WorkerControlHandler(
        inbox=inbox,
        conversations=None,
        clock=SimpleNamespace(now=lambda: NOW),
        orchestrator=None,
        background=None,
        owner=lambda: OWNER,
        session=lambda: None,
        update_actor=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control",
    [
        {"input_id": "c1", "kind": "something_new", "conversation_generation": 1},
        {"input_id": "c2", "kind": "reset_candidate", "conversation_generation": 1},
    ],
)
async def test_a_control_that_can_never_be_applied_is_closed_not_rescanned_forever(control):
    inbox = Inbox()
    await handler(inbox).handle(control)
    assert inbox.closed == [control["input_id"]]
