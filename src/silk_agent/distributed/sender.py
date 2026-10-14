"""Lease and deliver sanitized Outbox records in per-conversation order."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import timedelta
from typing import Any

from silk_agent.core.distributed_models import OutputDeliveryState
from silk_agent.core.distributed_ports import OutboxStore
from silk_agent.core.ports import Clock

SlackDelivery = Callable[[Mapping[str, Any]], Awaitable[Mapping[str, str] | None]]


class OutboxSender:
    def __init__(
        self,
        outbox: OutboxStore,
        clock: Clock,
        deliver: SlackDelivery,
        *,
        sender_id: str,
        lease_seconds: int = 30,
    ) -> None:
        if not sender_id or lease_seconds < 1:
            raise ValueError("sender_id and positive lease duration are required")
        self._outbox = outbox
        self._clock = clock
        self._deliver = deliver
        self._sender_id = sender_id
        self._lease_seconds = lease_seconds

    async def run_once(self, conversation_key: str) -> bool:
        now = self._clock.now()
        record = await self._outbox.claim_next(
            conversation_key=conversation_key,
            sender_id=self._sender_id,
            now=now,
            lease_expires_at=now + timedelta(seconds=self._lease_seconds),
        )
        if record is None:
            return False
        output_id = record.get("output_id")
        attempt_id = record.get("attempt_id")
        payload = record.get("payload")
        if (
            not isinstance(output_id, str)
            or not isinstance(attempt_id, str)
            or not isinstance(payload, Mapping)
        ):
            raise ValueError("claimed Outbox record is malformed")
        if not await self._outbox.revalidate_claim(
            output_id=output_id,
            attempt_id=attempt_id,
            now=self._clock.now(),
        ):
            return True
        try:
            slack_ref = await self._deliver(record)
        except Exception as exc:
            retry_after = _retry_after(exc)
            if retry_after is not None:
                now = self._clock.now()
                await self._outbox.mark_delivery(
                    output_id=output_id,
                    expected=OutputDeliveryState.DISPATCHING,
                    new_state=OutputDeliveryState.PENDING,
                    attempt_id=attempt_id,
                    next_attempt_at=now + timedelta(seconds=retry_after),
                    updated_at=now,
                )
                return True
            await self._outbox.mark_delivery(
                output_id=output_id,
                expected=OutputDeliveryState.DISPATCHING,
                new_state=OutputDeliveryState.UNCERTAIN,
                attempt_id=attempt_id,
                updated_at=self._clock.now(),
            )
            return True
        await self._outbox.mark_delivery(
            output_id=output_id,
            expected=OutputDeliveryState.DISPATCHING,
            new_state=OutputDeliveryState.DELIVERED,
            attempt_id=attempt_id,
            slack_ref=slack_ref,
            updated_at=self._clock.now(),
        )
        return True


def _retry_after(error: BaseException) -> float | None:
    response = getattr(error, "response", None)
    if getattr(response, "status_code", None) != 429:
        return None
    headers = getattr(response, "headers", {})
    raw = headers.get("Retry-After") if isinstance(headers, Mapping) else None
    if raw is None:
        return 1.0
    try:
        seconds = float(raw)
    except (TypeError, ValueError):
        return 1.0
    return max(0.0, seconds)
