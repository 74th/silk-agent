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
