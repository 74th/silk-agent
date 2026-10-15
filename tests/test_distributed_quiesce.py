import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.distributed.control_runtime import RecoveryLoop
from silk_agent.distributed.quiesce import QuiesceRequester, QuiesceResponder


class Clock:
    def now(self):
        return datetime(2026, 10, 15, tzinfo=UTC)


TOKEN = OwnershipToken("room", "worker-a", "inc-1", 3, 1, datetime(2026, 10, 14, tzinfo=UTC))


class Mailbox:
    def __init__(self):
        self.records = {}

    async def request(self, token, *, now):
        return self.records.setdefault(
            (token.conversation_key, token.epoch), {"state": "requested"}
        )

    async def get(self, token):
        return self.records.get((token.conversation_key, token.epoch))

    async def list_requested(self, *, worker_id, incarnation):
        return tuple(
            TOKEN
            for record in self.records.values()
            if record["state"] == "requested" and incarnation == TOKEN.worker_incarnation
        )

    async def answer(self, token, *, evidence, now):
        self.records[(token.conversation_key, token.epoch)] = {
            "state": "answered",
            "evidence": dict(evidence),
        }


class Worker:
    def __init__(self, evidence):
        self.evidence = evidence

    async def quiesce(self, token):
        return self.evidence


@pytest.mark.asyncio
async def test_requester_returns_the_evidence_a_live_worker_answers():
    mailbox = Mailbox()
    evidence = {"evidence_id": "e1", "worker_incarnation": "inc-1", "sdk_stopped": True}
    responder = QuiesceResponder(
        mailbox, Worker(evidence), Clock(), worker_id="worker-a", incarnation="inc-1"
    )
    requester = QuiesceRequester(mailbox, Clock(), wait_seconds=2, poll_seconds=0.01)
    pending = asyncio.create_task(requester(TOKEN))
    await asyncio.sleep(0.05)
    assert await responder.run_once() == 1
    assert await pending == evidence
    assert await responder.run_once() == 0  # answered requests are not served twice


@pytest.mark.asyncio
async def test_unanswered_or_unprovable_requests_yield_no_evidence():
    mailbox = Mailbox()
    requester = QuiesceRequester(mailbox, Clock(), wait_seconds=0.05, poll_seconds=0.01)
    assert await requester(TOKEN) is None  # dead Worker: nobody answers

    responder = QuiesceResponder(
        mailbox, Worker(None), Clock(), worker_id="worker-a", incarnation="inc-1"
    )
    assert await responder.run_once() == 0  # Worker cannot prove it stopped: stays unanswered
    assert mailbox.records[("room", 3)]["state"] == "requested"


@pytest.mark.asyncio
async def test_recovery_loop_scans_only_expired_ownership_and_isolates_failures():
    other = OwnershipToken("room-2", "worker-a", "inc-1", 1, 1, TOKEN.expires_at)

    class Ownership:
        def __init__(self):
            self.before = None

        async def list_expired(self, *, before, limit):
            self.before = before
            return (TOKEN, other)

    class Result:
        def __init__(self, state):
            self.state = state

    class Coordinator:
        def __init__(self):
            self.seen = []

        async def recover(self, token, *, request_quiesce):
            self.seen.append((token.conversation_key, request_quiesce))
            if token.conversation_key == "room-2":
                raise ConnectionError("store")
            return Result("released")

    ownership, coordinator = Ownership(), Coordinator()
    marker = object()
    loop = RecoveryLoop(ownership, coordinator, marker, Clock(), skew_seconds=5)
    assert await loop.run_once() == 1
    assert ownership.before == Clock().now() - timedelta(seconds=5)
    assert {key for key, _ in coordinator.seen} == {"room", "room-2"}
    assert all(sender is marker for _, sender in coordinator.seen)


@pytest.mark.asyncio
async def test_recovery_releases_an_expired_never_activated_assignment_without_evidence():
    class Ownership:
        async def list_expired(self, *, before, limit):
            return (TOKEN,)

        async def release_unactivated(self, token, *, now):
            return True

    class Coordinator:
        async def recover(self, *_args, **_kwargs):
            raise AssertionError("no stop evidence is needed for an assignment never activated")

    loop = RecoveryLoop(Ownership(), Coordinator(), object(), Clock())
    assert await loop.run_once() == 1
