"""Lease and deliver sanitized Outbox records in per-conversation order."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timedelta
from typing import Any, Literal

from silk_agent.core.distributed_models import OutputDeliveryState
from silk_agent.core.distributed_ports import OutboxStore
from silk_agent.core.ports import Clock
from silk_agent.distributed.metrics import METRICS

log = logging.getLogger(__name__)

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
            if retry_after is None:
                kind, retry_after = _classify_failure(exc, int(record.get("attempt_count") or 1))
                if kind == "failed":
                    METRICS.inc("outputs_total", result="failed")
                    log.warning(
                        "output %s was not posted and is given up (%s)",
                        output_id,
                        type(exc).__name__,
                    )
                    await self._outbox.mark_delivery(
                        output_id=output_id,
                        expected=OutputDeliveryState.DISPATCHING,
                        new_state=OutputDeliveryState.FAILED,
                        attempt_id=attempt_id,
                        updated_at=self._clock.now(),
                    )
                    return True
            if retry_after is not None:
                METRICS.inc("outputs_total", result="retry")
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
            METRICS.inc("outputs_total", result="uncertain")
            await self._outbox.mark_delivery(
                output_id=output_id,
                expected=OutputDeliveryState.DISPATCHING,
                new_state=OutputDeliveryState.UNCERTAIN,
                attempt_id=attempt_id,
                updated_at=self._clock.now(),
            )
            return True
        METRICS.inc("outputs_total", result="delivered")
        created = record.get("created_at")
        if isinstance(created, str):
            try:
                created = datetime.fromisoformat(created)
            except ValueError:
                created = None
        if isinstance(created, datetime) and created.tzinfo is not None:
            METRICS.observe(
                "output_delivery_latency_seconds",
                max(0.0, (self._clock.now() - created).total_seconds()),
            )
        await self._outbox.mark_delivery(
            output_id=output_id,
            expected=OutputDeliveryState.DISPATCHING,
            new_state=OutputDeliveryState.DELIVERED,
            attempt_id=attempt_id,
            slack_ref=slack_ref,
            updated_at=self._clock.now(),
        )
        return True


# Slack answered `ok: false`, so nothing was posted. These codes are worth another try; any
# other code (channel_not_found, is_archived, msg_too_long, ...) will not change.
_TRANSIENT_SLACK_ERRORS = frozenset(
    {"internal_error", "service_unavailable", "fatal_error", "request_timeout"}
)
MAX_DELIVERY_ATTEMPTS = 10
_BACKOFF_CAP_SECONDS = 60.0


def _classify_failure(
    error: BaseException, attempt: int
) -> tuple[Literal["retry", "failed", "uncertain"], float | None]:
    """What a failed send means for the output.

    "failed": known not to have been posted (Slack rejected it, or it was never sent because
    the payload is malformed) and not worth retrying. "retry": the same, but transient: try
    again after the returned delay (until MAX_DELIVERY_ATTEMPTS). "uncertain": the request may
    have reached Slack (a lost response); resending could post twice.
    """
    if isinstance(error, ValueError | TypeError | KeyError):
        return "failed", None  # raised while building the message, before anything was sent
    response = getattr(error, "response", None)
    if response is not None and _is_not_ok(response):
        code = str(response["error"])
        if code in _TRANSIENT_SLACK_ERRORS and attempt < MAX_DELIVERY_ATTEMPTS:
            return "retry", min(2.0 ** max(0, attempt - 1), _BACKOFF_CAP_SECONDS)
        return "failed", None
    return "uncertain", None


def _is_not_ok(response: Any) -> bool:
    try:
        return response["ok"] is False and isinstance(response["error"], str)
    except (KeyError, TypeError, IndexError):
        return False


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
