import json
import logging
from datetime import UTC, datetime

import pytest
from aiohttp.test_utils import TestClient, TestServer

from silk_agent.core.distributed_models import OutputDeliveryState
from silk_agent.distributed.control_runtime import ControlAllocatorLoop
from silk_agent.distributed.health import RoleHealth, build_health_app
from silk_agent.distributed.logging import RoleLogFormatter
from silk_agent.distributed.metrics import METRICS, Metrics
from silk_agent.distributed.sender import OutboxSender
from silk_agent.distributed.worker import DistributedWorker
from tests.test_distributed_worker import Actor, Clock, Ownership, no_watch, token


@pytest.fixture(autouse=True)
def fresh_metrics():
    METRICS.reset()
    yield
    METRICS.reset()


def test_metrics_render_counters_gauges_and_histograms_with_bounded_labels():
    metrics = Metrics()
    metrics.describe("things_total", "Things.")
    metrics.inc("things_total", result="ok")
    metrics.inc("things_total", 2, result="ok")
    metrics.set("depth", 3)
    metrics.observe("latency_seconds", 0.4, kind='a"b')
    text = metrics.render()
    assert 'silk_agent_things_total{result="ok"} 3' in text
    assert "silk_agent_depth 3" in text
    assert 'silk_agent_latency_seconds_bucket{kind="a\\"b",le="0.5"} 1' in text
    assert 'silk_agent_latency_seconds_count{kind="a\\"b"} 1' in text
    assert metrics.value("things_total", result="ok") == 3


def test_metrics_drop_new_series_beyond_the_cap_instead_of_growing():
    metrics = Metrics()
    for index in range(2000):
        metrics.inc("per_conversation_total", conversation=str(index))
    assert metrics.render().count("per_conversation_total{") <= 500


def test_loop_liveness_and_readiness_follow_ticks():
    now = [100.0]
    health = RoleHealth("worker", monotonic=lambda: now[0])
    tick = health.loop("heartbeat", 60)
    assert health.not_ready() == ["heartbeat_started"] and health.stalled() == []
    tick()
    assert health.not_ready() == []
    now[0] += 61
    assert health.stalled() == ["heartbeat"]
    tick()
    assert health.stalled() == []


@pytest.mark.asyncio
async def test_health_endpoints_report_stalled_loops_readiness_and_metrics():
    now = [0.0]
    health = RoleHealth("control", monotonic=lambda: now[0])
    tick = health.loop("sender", 10)
    metrics = Metrics()
    metrics.inc("outputs_total", result="delivered")
    async with TestClient(TestServer(build_health_app(health, metrics))) as client:
        assert (await client.get("/readyz")).status == 503
        tick()
        assert (await client.get("/readyz")).status == 200
        assert (await client.get("/healthz")).status == 200
        now[0] = 11
        stalled = await client.get("/healthz")
        assert stalled.status == 503 and (await stalled.json())["loops"] == ["sender"]
        body = await (await client.get("/metrics")).text()
        assert 'silk_agent_outputs_total{result="delivered"} 1' in body


@pytest.mark.asyncio
async def test_lease_loss_and_renewal_errors_are_observable_without_conversation_labels():
    clock = Clock()
    ownership = Ownership([token("conversation-a")])

    class Unavailable(Ownership):
        async def renew(self, *args, **kwargs):
            raise ConnectionError("store offline")

    ownership = Unavailable([token("conversation-a")])
    worker = DistributedWorker(
        ownership,
        clock,
        worker_id="worker-a",
        worker_incarnation="pod-a:boot-1",
        actor_factory=Actor,
        subscribe=no_watch,
    )
    await worker.run_once()
    await worker._renew_actors()
    assert METRICS.value("worker_lease_renewal_failures_total", reason="error") == 1
    assert METRICS.value("worker_actor_starts_total", kind="sdk") == 1
    assert "conversation-a" not in METRICS.render()


@pytest.mark.asyncio
async def test_uncertain_output_and_waiting_conversations_are_counted():
    class Outbox:
        def __init__(self):
            self.record = {"output_id": "o1", "attempt_id": "a1", "payload": {}}

        async def claim_next(self, **_):
            record, self.record = self.record, None
            return record

        async def mark_delivery(self, **kwargs):
            assert kwargs["new_state"] is OutputDeliveryState.UNCERTAIN
            return True

        async def revalidate_claim(self, **_):
            return True

    async def lost(_record):
        raise ConnectionError("accepted but connection dropped")

    class SenderClock:
        def now(self):
            return datetime(2026, 10, 15, tzinfo=UTC)

    await OutboxSender(Outbox(), SenderClock(), lost, sender_id="s").run_once("room")
    assert METRICS.value("outputs_total", result="uncertain") == 1

    class Conversations:
        async def list_waiting(self, *, limit):
            return ("a", "b", "c")

    class Workers:
        async def list_available(self, *, now):
            return ()

    loop = ControlAllocatorLoop(
        Conversations(), Workers(), object(), SenderClock(), allocator_id="x"
    )
    assert await loop.run_once() == 0  # no Worker: nothing is assigned, the queue is not hidden
    assert METRICS.value("control_waiting_conversations") == 3
    assert METRICS.value("control_available_workers") == 0


def test_role_logs_keep_ids_but_never_bodies_or_tokens():
    record = logging.LogRecord(
        "x", logging.WARNING, "f", 1, "pass failed (%s)", ("ValueError",), None
    )
    record.request_id = "req-1"
    record.text = "secret user message"
    record.authorization = "Bearer abc"
    output = RoleLogFormatter().format(record)
    parsed = json.loads(output)
    assert parsed["request_id"] == "req-1"
    assert "secret" not in output and "Bearer" not in output
