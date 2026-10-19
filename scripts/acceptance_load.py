"""Slack-free load measurement against the real cluster (real Firestore, SDK, Sandboxes).

Runs N conversations at once, twice with the same conversation keys: the first pass pays the
cold Sandbox start, the second is warm. Prints per-pass completion latency, the Workers'
memory/CPU sampled from `kubectl top`, and the Workers' own latency metrics.

    uv run python scripts/acceptance_load.py [CONVERSATIONS] [SECONDS_PER_COMMAND]

Outputs of these test conversations are marked delivered so Control never posts them to Slack.
"""

import asyncio
import statistics
import subprocess
import sys
import time
from datetime import timedelta

sys.path.insert(0, __import__("os").path.dirname(__file__))
from acceptance_common import *  # noqa: E402,F403

NS = "session-sandbox-system"


def _kubectl(*args: str) -> str:
    return subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True).stdout


def sample_top() -> list[tuple[str, int, int]]:
    rows = []
    for line in _kubectl("top", "pods", "--no-headers").splitlines():
        name, cpu, memory = line.split()[:3]
        if "worker" in name:
            rows.append((name[-5:], int(cpu.rstrip("m")), int(memory.rstrip("Mi"))))
    return rows


def worker_metrics() -> dict[str, float]:
    pods = _kubectl("get", "pods", "-l", "app.kubernetes.io/name=silk-agent-worker", "-o", "name")
    totals: dict[str, float] = {}
    for pod in pods.split():
        text = _kubectl(
            "exec",
            pod.removeprefix("pod/"),
            "--",
            "python",
            "-c",
            "import urllib.request;print(urllib.request.urlopen('http://localhost:9090/metrics')"
            ".read().decode())",
        )
        for line in text.splitlines():
            if line.startswith("#") or not line.startswith("silk_agent_"):
                continue
            name, _, value = line.rpartition(" ")
            if any(
                key in name
                for key in (
                    "input_dispatch_latency_seconds_sum",
                    "input_dispatch_latency_seconds_count",
                    "worker_lease_renewal_failures_total",
                    "inputs_uncertain_total",
                )
            ):
                totals[name] = totals.get(name, 0.0) + float(value)
    return totals


async def one_pass(e, keys, seconds: int, label: str) -> list[float]:
    started = {}
    for key in keys:
        started[key] = time.time()
        await e.send(
            key,
            f"シェルで `sleep {seconds}` を実行して、終わったら ok と答えてください。",
        )
    done: dict[str, float] = {}
    peak: dict[str, tuple[int, int]] = {}
    deadline = time.time() + 900
    last_sample = 0.0
    while len(done) < len(keys) and time.time() < deadline:
        await e.loop.run_once()
        for key in keys:
            if key in done:
                continue
            outputs = await e.outputs(key)
            if kinds(outputs).count("TurnFinished") >= e.finished_before[key] + 1:
                done[key] = time.time() - started[key]
        if time.time() - last_sample > 10:
            last_sample = time.time()
            for pod, cpu, memory in sample_top():
                best = peak.get(pod, (0, 0))
                peak[pod] = (max(best[0], cpu), max(best[1], memory))
        await asyncio.sleep(2)
    latencies = sorted(done.values())
    print(f"\n[{label}] {len(done)}/{len(keys)} finished")
    if latencies:
        print(
            f"  completion seconds: min {latencies[0]:.0f}  "
            f"median {statistics.median(latencies):.0f}  max {latencies[-1]:.0f}"
        )
    print("  worker peak (cpu m, memory Mi):", {pod: v for pod, v in sorted(peak.items())})
    for key in keys:
        e.finished_before[key] = kinds(await e.outputs(key)).count("TurnFinished")
    return latencies


async def suppress_outputs(e, prefix: str) -> None:
    expire_at = datetime.now(UTC) + timedelta(days=1)  # test records are not kept
    async for doc in e.c.collection(f"{P}_outbox").stream():
        data = doc.to_dict()
        if (
            data.get("conversation_key", "").startswith(prefix)
            and data.get("status") != "delivered"
        ):
            await doc.reference.update(
                {"status": "delivered", "suppressed_reason": "load-test", "expire_at": expire_at}
            )


async def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    seconds = int(sys.argv[2]) if len(sys.argv) > 2 else 15
    e = Env()
    e.finished_before = {}
    keys = [f"loadtest:{index:02d}" for index in range(count)]
    for key in keys:
        e.finished_before[key] = kinds(await e.outputs(key)).count("TurnFinished")
    print("workers:", [w.worker_id[-5:] for w in await e.d.list_available(now=SystemClock().now())])
    print("metrics before:", worker_metrics())
    try:
        await one_pass(e, keys, seconds, "pass 1 (cold Sandbox start)")
        await one_pass(e, keys, seconds, "pass 2 (warm)")
        print("\nmetrics after:", worker_metrics())
    finally:
        await suppress_outputs(e, "loadtest:")


asyncio.run(main())
