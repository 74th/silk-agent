import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.distributed.control_runtime import RecoveryLoop
from silk_agent.distributed.pod_probe import KubernetesPodProbe, pod_uid_of

NOW = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
UID = "uid-1"


def token(expired_for=300):
    return OwnershipToken(
        "slack:T:C:1",
        "worker-pod-a",
        json.dumps([UID, "boot"]),
        3,
        1,
        NOW - timedelta(seconds=expired_for),
    )


class Clock:
    def now(self):
        return NOW


def test_pod_uid_is_read_from_the_incarnation():
    assert pod_uid_of(token()) == UID
    assert pod_uid_of(OwnershipToken("k", "w", "not-json", 1, 1, NOW)) is None


class Api:
    def __init__(self, result):
        self.result = result

    async def read_namespaced_pod(self, name, namespace):
        assert (name, namespace) == ("worker-pod-a", "ns")
        if isinstance(self.result, Exception):
            raise self.result
        return SimpleNamespace(metadata=SimpleNamespace(uid=self.result))


class ApiError(Exception):
    def __init__(self, status):
        self.status = status


@pytest.mark.asyncio
async def test_pod_is_gone_only_for_a_404_or_a_different_uid():
    assert await KubernetesPodProbe(Api(ApiError(404)), "ns").is_gone(token())
    assert await KubernetesPodProbe(Api("uid-2"), "ns").is_gone(token())  # name reused
    assert not await KubernetesPodProbe(Api(UID), "ns").is_gone(token())
    # An API error is not evidence of absence.
    assert not await KubernetesPodProbe(Api(ApiError(500)), "ns").is_gone(token())
    assert not await KubernetesPodProbe(Api(ConnectionError("down")), "ns").is_gone(token())


class Ownership:
    def __init__(self, expired):
        self.expired = expired
        self.confirmed = []

    async def list_expired(self, *, before, limit):
        return (self.expired,)

    async def release_unactivated(self, token, *, now):
        return False

    async def confirm_stopped(self, token, **kwargs):
        self.confirmed.append(kwargs["evidence_id"])
        return True


class Blocked:
    state = "blocked"


class Coordinator:
    async def recover(self, token, *, request_quiesce):
        return Blocked()


def loop(ownership, *, gone, after=120, notified=None):
    async def pod_gone(_token):
        return gone

    async def notify(tok):
        notified.append(tok.conversation_key)

    return RecoveryLoop(
        ownership,
        Coordinator(),
        object(),
        Clock(),
        pod_gone=pod_gone,
        auto_recover_after_seconds=after,
        notify_blocked=notify if notified is not None else None,
    )


@pytest.mark.asyncio
async def test_missing_pod_releases_after_the_wait_with_auto_evidence():
    ownership = Ownership(token(expired_for=300))
    assert await loop(ownership, gone=True).run_once() == 1
    assert ownership.confirmed == ["auto:pod-absent:worker-pod-a"]


@pytest.mark.asyncio
async def test_a_live_pod_or_too_recent_expiry_stays_blocked_and_the_user_is_told_once_per_call():
    notified = []
    live = Ownership(token(expired_for=300))
    assert await loop(live, gone=False, notified=notified).run_once() == 0
    recent = Ownership(token(expired_for=30))  # inside the self-stop window
    assert await loop(recent, gone=True, notified=notified).run_once() == 0
    assert live.confirmed == [] and recent.confirmed == []
    assert notified == ["slack:T:C:1", "slack:T:C:1"]


@pytest.mark.asyncio
async def test_feature_is_off_without_a_probe():
    ownership = Ownership(token(expired_for=999))
    plain = RecoveryLoop(ownership, Coordinator(), object(), Clock())
    assert await plain.run_once() == 0
    assert ownership.confirmed == []


class MovingClock:
    def __init__(self):
        self.value = NOW

    def now(self):
        return self.value


class CountingCoordinator:
    def __init__(self):
        self.calls = 0

    async def recover(self, token, *, request_quiesce):
        self.calls += 1
        return Blocked()


@pytest.mark.asyncio
async def test_recovery_only_runs_while_an_input_is_waiting():
    waiting = {"value": False}
    coordinator = CountingCoordinator()
    clock = MovingClock()
    queries = []

    async def has_demand(tok):
        queries.append(tok.conversation_key)
        return waiting["value"]

    recovery = RecoveryLoop(
        Ownership(token()),
        coordinator,
        object(),
        clock,
        has_demand=has_demand,
        idle_check_seconds=30,
    )
    for _ in range(5):  # polling with nobody waiting never touches the stopped Worker
        await recovery.run_once()
    assert coordinator.calls == 0 and len(queries) == 1  # and re-checks only every 30 s
    clock.value += timedelta(seconds=31)
    waiting["value"] = True  # a user message arrives
    await recovery.run_once()
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_failed_recovery_backs_off_exponentially_up_to_a_cap():
    coordinator = CountingCoordinator()
    clock = MovingClock()

    async def has_demand(_tok):
        return True

    recovery = RecoveryLoop(
        Ownership(token()),
        coordinator,
        object(),
        clock,
        has_demand=has_demand,
        backoff_base_seconds=5,
        backoff_max_seconds=40,
    )
    attempts = []
    for second in range(0, 200):
        clock.value = NOW + timedelta(seconds=second)
        before = coordinator.calls
        await recovery.run_once()
        if coordinator.calls > before:
            attempts.append(second)
    gaps = [b - a for a, b in zip(attempts, attempts[1:], strict=False)]
    assert gaps[:4] == [5, 10, 20, 40] and set(gaps[3:]) == {40}
    assert len(attempts) < 12  # not one attempt per poll


def replaced_loop(ownership, current, *, gone=False, after=120):
    async def current_incarnation(worker_id):
        assert worker_id == "worker-pod-a"
        return current

    async def pod_gone(_token):
        return gone

    return RecoveryLoop(
        ownership,
        Coordinator(),
        object(),
        Clock(),
        pod_gone=pod_gone,
        current_incarnation=current_incarnation,
        auto_recover_after_seconds=after,
    )


@pytest.mark.asyncio
async def test_a_newer_incarnation_of_the_same_pod_releases_even_though_the_pod_exists():
    # The container restarted (OOM): same Pod, same UID, new incarnation. Kubernetes still
    # shows the Pod, but the old process is gone.
    ownership = Ownership(token(expired_for=300))
    newer = json.dumps([UID, "boot-2"])
    assert await replaced_loop(ownership, newer, gone=False).run_once() == 1
    assert ownership.confirmed == ["auto:incarnation-replaced:worker-pod-a"]


@pytest.mark.asyncio
async def test_the_same_incarnation_or_another_pod_is_not_evidence():
    same = Ownership(token(expired_for=300))
    assert await replaced_loop(same, token().worker_incarnation).run_once() == 0
    other_pod = Ownership(token(expired_for=300))
    assert await replaced_loop(other_pod, json.dumps(["uid-2", "boot"])).run_once() == 0
    no_record = Ownership(token(expired_for=300))
    assert await replaced_loop(no_record, None).run_once() == 0
    assert same.confirmed == other_pod.confirmed == no_record.confirmed == []


@pytest.mark.asyncio
async def test_a_newer_incarnation_still_waits_for_the_self_stop_window():
    ownership = Ownership(token(expired_for=30))
    assert await replaced_loop(ownership, json.dumps([UID, "boot-2"])).run_once() == 0
    assert ownership.confirmed == []


class PagedOwnership(Ownership):
    """A stable-order collection of expired ownerships, served in pages like Firestore."""

    def __init__(self, count):
        self.tokens = [
            OwnershipToken(
                f"slack:T:C:{n}",
                "worker-pod-a",
                json.dumps([UID, "boot"]),
                3,
                1,
                NOW - timedelta(seconds=300),
            )
            for n in range(count)
        ]
        self.confirmed = []

    async def list_expired_page(self, *, before, limit, after):
        start = 0 if after is None else after
        page = self.tokens[start : start + limit]
        nxt = start + limit
        return tuple(page), (nxt if nxt < len(self.tokens) else None)


@pytest.mark.asyncio
async def test_an_ownership_nobody_waits_on_cannot_hide_a_later_one_that_is_waited_on():
    # More expired ownerships than one page, and the only one with a waiting input is last.
    ownership = PagedOwnership(5)
    asked = []

    async def has_demand(tok):
        asked.append(tok.conversation_key)
        return tok.conversation_key == "slack:T:C:4"

    recovery = RecoveryLoop(
        ownership,
        Coordinator(),
        object(),
        Clock(),
        batch_size=2,
        has_demand=has_demand,
        current_incarnation=None,
    )
    for _ in range(3):  # three pages: 0-1, 2-3, 4
        await recovery.run_once()
    assert "slack:T:C:4" in asked


@pytest.mark.asyncio
async def test_pending_recovery_gauge_ignores_ownerships_nobody_waits_on():
    from silk_agent.distributed.metrics import METRICS

    METRICS.reset()
    waiting = {"value": False}

    async def has_demand(_tok):
        return waiting["value"]

    recovery = RecoveryLoop(
        Ownership(token()), CountingCoordinator(), object(), MovingClock(), has_demand=has_demand
    )
    await recovery.run_once()
    assert METRICS.value("control_recovery_pending") == 0  # expired, but no input is waiting
    waiting["value"] = True
    clock = recovery._clock
    clock.value += timedelta(seconds=31)  # past the idle re-check
    await recovery.run_once()
    assert METRICS.value("control_recovery_pending") == 1  # a user waits for stop evidence
    assert recovery._coordinator.calls == 1
    for _ in range(3):  # backing off: no retry, yet the wait is still reported
        await recovery.run_once()
        assert METRICS.value("control_recovery_pending") == 1
    assert recovery._coordinator.calls == 1
    recovery._ownership.expired = None  # released or renewed: nothing is expired any more
    recovery._ownership.list_expired = _no_expired
    clock.value += timedelta(seconds=600)
    await recovery.run_once()
    assert METRICS.value("control_recovery_pending") == 0
    METRICS.reset()


async def _no_expired(*, before, limit):
    return ()
