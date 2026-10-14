"""Recover expired Worker ownership only after explicit old-process stop proof."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Literal

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.distributed_ports import OwnershipStore
from silk_agent.core.ports import Clock


@dataclass(frozen=True)
class RecoveryResult:
    state: Literal["blocked", "released"]
    evidence_id: str | None = None


QuiesceRequest = Callable[[OwnershipToken], Awaitable[Mapping[str, object] | None]]


class DistributedRecoveryCoordinator:
    """Keep a conversation unavailable until its prior SDK process proves closure."""

    def __init__(self, ownership: OwnershipStore, clock: Clock) -> None:
        self._ownership = ownership
        self._clock = clock

    async def recover(
        self,
        token: OwnershipToken,
        *,
        request_quiesce: QuiesceRequest,
    ) -> RecoveryResult | None:
        now = self._clock.now()
        if not await self._ownership.begin_recovery(token, now=now):
            return None
        try:
            evidence = await request_quiesce(token)
        except Exception:
            evidence = None
        evidence_id = evidence.get("evidence_id") if isinstance(evidence, Mapping) else None
        incarnation = evidence.get("worker_incarnation") if isinstance(evidence, Mapping) else None
        sdk_stopped = evidence.get("sdk_stopped") if isinstance(evidence, Mapping) else False
        if (
            not sdk_stopped
            or not isinstance(evidence_id, str)
            or not evidence_id
            or incarnation != token.worker_incarnation
        ):
            await self._ownership.mark_recovery_blocked(
                token, reason="old_worker_not_stopped", now=self._clock.now()
            )
            return RecoveryResult("blocked")
        if not await self._ownership.confirm_stopped(
            token,
            evidence_id=evidence_id,
            worker_incarnation=token.worker_incarnation,
            now=self._clock.now(),
        ):
            await self._ownership.mark_recovery_blocked(
                token, reason="stop_evidence_rejected", now=self._clock.now()
            )
            return RecoveryResult("blocked")
        return RecoveryResult("released", evidence_id)
