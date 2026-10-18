"""Worker process identity and liveness registration."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from uuid import uuid4

from silk_agent.core.distributed_models import WorkerRecord
from silk_agent.core.distributed_ports import WorkerDirectory
from silk_agent.core.ports import Clock
from silk_agent.stores.firestore.ids import worker_incarnation

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerIdentity:
    worker_id: str
    incarnation: str

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> WorkerIdentity:
        values = os.environ if environ is None else environ
        pod_name = values.get("POD_NAME", "")
        pod_uid = values.get("POD_UID", "")
        if not pod_name or not pod_uid:
            raise RuntimeError("POD_NAME and POD_UID are required for a Worker process")
        return cls(pod_name, worker_incarnation(pod_uid, uuid4().hex))


WorkerUsage = Callable[[], tuple[int, int]]


class WorkerHeartbeat:
    """Register one incarnation and refresh liveness without changing admin drain state."""

    def __init__(
        self,
        directory: WorkerDirectory,
        clock: Clock,
        identity: WorkerIdentity,
        *,
        capacity: int,
        usage: WorkerUsage,
        interval_seconds: float = 20,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        if capacity < 1 or interval_seconds <= 0:
            raise ValueError("capacity and heartbeat interval must be positive")
        self._directory = directory
        self._clock = clock
        self._identity = identity
        self._capacity = capacity
        self._usage = usage
        self._interval_seconds = interval_seconds
        self._on_tick = on_tick

    async def register(self) -> None:
        reserved, active_sdk = self._read_usage()
        record = self._record(reserved, active_sdk)
        if not await self._directory.register(record):
            raise RuntimeError("Worker incarnation is already registered")

    async def heartbeat_once(self) -> bool:
        reserved, active_sdk = self._read_usage()
        return await self._directory.heartbeat(self._record(reserved, active_sdk))

    async def run(self, stop: asyncio.Event) -> None:
        await self.register()
        await self.run_registered(stop)

    async def run_registered(self, stop: asyncio.Event) -> None:
        """Refresh liveness after another coordinator registered this incarnation."""
        while not stop.is_set():
            if self._on_tick is not None:
                self._on_tick()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval_seconds)
                break
            except TimeoutError:
                pass
            try:
                if not await self.heartbeat_once():
                    raise RuntimeError("Worker heartbeat was rejected for this incarnation")
            except asyncio.CancelledError:
                raise
            except RuntimeError:
                raise
            except Exception as exc:
                log.warning("Worker heartbeat failed (%s)", type(exc).__name__)

    def _read_usage(self) -> tuple[int, int]:
        reserved, active_sdk = self._usage()
        if (
            isinstance(reserved, bool)
            or not isinstance(reserved, int)
            or isinstance(active_sdk, bool)
            or not isinstance(active_sdk, int)
            or reserved < 0
            or active_sdk < 0
            or reserved > self._capacity
            or active_sdk > self._capacity
        ):
            raise ValueError("Worker usage is outside configured capacity")
        return reserved, active_sdk

    def _record(self, reserved: int, active_sdk: int) -> WorkerRecord:
        return WorkerRecord(
            worker_id=self._identity.worker_id,
            incarnation=self._identity.incarnation,
            capacity=self._capacity,
            reserved=reserved,
            active_sdk=active_sdk,
            draining=False,
            heartbeat_at=self._clock.now(),
        )
