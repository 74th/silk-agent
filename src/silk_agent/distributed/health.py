"""Liveness / readiness / metrics endpoint shared by the Gateway, Worker and Control roles."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

from aiohttp import web

from silk_agent.distributed.metrics import METRICS, Metrics

log = logging.getLogger(__name__)


class RoleHealth:
    """Track loop progress (liveness) and named dependency conditions (readiness)."""

    def __init__(
        self,
        role: str,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.role = role
        self._monotonic = monotonic
        self._loops: dict[str, tuple[float, float]] = {}  # name -> (last beat, max age)
        self._conditions: dict[str, bool] = {}

    def expect(self, name: str, max_age_seconds: float) -> None:
        """Declare a loop that must make progress at least every ``max_age_seconds``."""
        self._loops[name] = (self._monotonic(), max_age_seconds)

    def loop(self, name: str, max_age_seconds: float) -> Callable[[], None]:
        """Declare a loop and return its tick: liveness beat plus a first-pass readiness mark."""
        self.expect(name, max_age_seconds)
        self.set_ready(f"{name}_started", False)

        def tick() -> None:
            self.beat(name)
            self._conditions[f"{name}_started"] = True

        return tick

    def beat(self, name: str) -> None:
        if name in self._loops:
            self._loops[name] = (self._monotonic(), self._loops[name][1])

    def set_ready(self, name: str, ready: bool) -> None:
        self._conditions[name] = ready

    def stalled(self) -> list[str]:
        now = self._monotonic()
        return sorted(name for name, (last, limit) in self._loops.items() if now - last > limit)

    def not_ready(self) -> list[str]:
        return sorted(name for name, ready in self._conditions.items() if not ready)


def build_health_app(health: RoleHealth, metrics: Metrics = METRICS) -> web.Application:
    async def healthz(_request: web.Request) -> web.Response:
        stalled = health.stalled()
        if stalled:
            # Names are fixed loop identifiers, never conversation data.
            return web.json_response({"status": "stalled", "loops": stalled}, status=503)
        return web.json_response({"status": "ok", "role": health.role})

    async def readyz(_request: web.Request) -> web.Response:
        waiting = health.not_ready()
        if waiting:
            return web.json_response({"status": "not_ready", "conditions": waiting}, status=503)
        return web.json_response({"status": "ready", "role": health.role})

    async def scrape(_request: web.Request) -> web.Response:
        return web.Response(text=metrics.render(), content_type="text/plain")

    app = web.Application()
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/readyz", readyz)
    app.router.add_get("/metrics", scrape)
    return app


async def serve_health(
    health: RoleHealth, port: int, stop: asyncio.Event, metrics: Metrics = METRICS
) -> None:
    """Serve until ``stop``; the endpoint runs on its own port, away from role traffic."""
    runner = web.AppRunner(build_health_app(health, metrics), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    log.info("health endpoint listening", extra={"port": port})
    try:
        await stop.wait()
    finally:
        await runner.cleanup()


def process_memory_collector(metrics: Metrics) -> None:
    """Resident set size from /proc (Linux); skipped where unavailable."""
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            pages = int(statm.read().split()[1])
    except (OSError, ValueError, IndexError):
        return
    import os

    metrics.set("process_resident_memory_bytes", pages * os.sysconf("SC_PAGE_SIZE"))
