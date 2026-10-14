import asyncio
from datetime import UTC, datetime

import pytest

from silk_agent.distributed.worker_heartbeat import WorkerHeartbeat, WorkerIdentity


class FakeClock:
    def now(self):
        return datetime(2026, 10, 14, tzinfo=UTC)


class FakeDirectory:
    def __init__(self):
        self.registered = []
        self.heartbeats = []
        self.reject_register = False
        self.reject_heartbeat = False

    async def register(self, record):
        self.registered.append(record)
        return not self.reject_register

    async def heartbeat(self, record):
        self.heartbeats.append(record)
        return not self.reject_heartbeat


def test_worker_identity_requires_kubernetes_pod_name_and_uid():
    identity = WorkerIdentity.from_environment({"POD_NAME": "worker-0", "POD_UID": "uid-1"})

    assert identity.worker_id == "worker-0"
    assert '"uid-1"' in identity.incarnation
    with pytest.raises(RuntimeError, match="POD_NAME and POD_UID"):
        WorkerIdentity.from_environment({"POD_NAME": "worker-0"})


@pytest.mark.asyncio
async def test_worker_heartbeat_registers_and_reports_usage():
    directory = FakeDirectory()
    heartbeat = WorkerHeartbeat(
        directory,
        FakeClock(),
        WorkerIdentity("worker-0", "incarnation-1"),
        capacity=4,
        usage=lambda: (2, 1),
    )

    await heartbeat.register()
    assert directory.registered[0].reserved == 2
    assert directory.registered[0].active_sdk == 1
    assert await heartbeat.heartbeat_once()
    assert directory.heartbeats[0].incarnation == "incarnation-1"


@pytest.mark.asyncio
async def test_worker_heartbeat_stops_if_registration_is_rejected():
    directory = FakeDirectory()
    directory.reject_register = True
    heartbeat = WorkerHeartbeat(
        directory,
        FakeClock(),
        WorkerIdentity("worker-0", "incarnation-1"),
        capacity=1,
        usage=lambda: (0, 0),
    )

    with pytest.raises(RuntimeError, match="already registered"):
        await heartbeat.run(asyncio.Event())


def test_worker_heartbeat_rejects_usage_above_capacity():
    heartbeat = WorkerHeartbeat(
        FakeDirectory(),
        FakeClock(),
        WorkerIdentity("worker-0", "incarnation-1"),
        capacity=1,
        usage=lambda: (2, 0),
    )

    with pytest.raises(ValueError, match="outside configured capacity"):
        heartbeat._read_usage()
