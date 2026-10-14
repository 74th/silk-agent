"""Quiesce request/response between Control recovery and the (possibly returning) Worker."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Any, Protocol

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.ports import Clock

log = logging.getLogger(__name__)


class Mailbox(Protocol):
    async def request(self, token: OwnershipToken, *, now: Any) -> Mapping[str, Any]: ...

    async def get(self, token: OwnershipToken) -> Mapping[str, Any] | None: ...

    async def list_requested(
        self, *, worker_id: str, incarnation: str
    ) -> tuple[OwnershipToken, ...]: ...

    async def answer(
        self, token: OwnershipToken, *, evidence: Mapping[str, object], now: Any
    ) -> None: ...


class QuiesceRequester:
    """Control side: ask the old incarnation and wait a bounded time for SDK-closed evidence.

    No answer is not proof of anything; the caller keeps the conversation blocked.
    """

    def __init__(
        self,
        mailbox: Mailbox,
        clock: Clock,
        *,
        wait_seconds: float = 15,
        poll_seconds: float = 0.5,
    ) -> None:
        if wait_seconds <= 0 or poll_seconds <= 0:
            raise ValueError("quiesce wait values must be positive")
        self._mailbox = mailbox
        self._clock = clock
        self._wait = wait_seconds
        self._poll = poll_seconds

    async def __call__(self, token: OwnershipToken) -> Mapping[str, object] | None:
        existing = await self._mailbox.request(token, now=self._clock.now())
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._wait
        record: Mapping[str, Any] | None = existing
        while True:
            if record is not None and record.get("state") == "answered":
                evidence = record.get("evidence")
                return evidence if isinstance(evidence, Mapping) else None
            if loop.time() >= deadline:
                return None
            await asyncio.sleep(self._poll)
            record = await self._mailbox.get(token)


class Quiescer(Protocol):
    async def quiesce(self, token: OwnershipToken) -> dict[str, object] | None: ...


class QuiesceResponder:
    """Worker side: answer requests addressed to this incarnation with local stop evidence."""

    def __init__(
        self,
        mailbox: Mailbox,
        worker: Quiescer,
        clock: Clock,
        *,
        worker_id: str,
        incarnation: str,
        poll_seconds: float = 2,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        self._mailbox = mailbox
        self._worker = worker
        self._clock = clock
        self._worker_id = worker_id
        self._incarnation = incarnation
        self._poll = poll_seconds

    async def run_once(self) -> int:
        answered = 0
        for token in await self._mailbox.list_requested(
            worker_id=self._worker_id, incarnation=self._incarnation
        ):
            evidence = await self._worker.quiesce(token)
            if evidence is None:
                continue  # nothing proves the SDK is closed; leave the request unanswered
            await self._mailbox.answer(token, evidence=evidence, now=self._clock.now())
            answered += 1
        return answered

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("quiesce responder pass failed (%s)", type(exc).__name__)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._poll)
            except TimeoutError:
                pass
