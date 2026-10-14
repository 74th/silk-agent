"""Persist already sanitized conversation events for an independent Slack Sender."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.events import OutboundEvent
from silk_agent.core.ports import Clock
from silk_agent.stores.firestore.outbox import FirestoreOutboxWriter


class OutputSequence(Protocol):
    async def __call__(self, conversation_key: str) -> int: ...


class OwnerForConversation(Protocol):
    async def __call__(self, conversation_key: str) -> OwnershipToken | None: ...


class OriginSequence(Protocol):
    async def __call__(self, event: OutboundEvent) -> int | None: ...


class SanitizedOutboxSink:
    """Adapter used as ConversationOrchestrator.outbox_sink."""

    def __init__(
        self,
        writer: FirestoreOutboxWriter,
        clock: Clock,
        owner_for: OwnerForConversation,
        next_sequence: OutputSequence,
        origin_sequence: OriginSequence | None = None,
    ) -> None:
        self._writer = writer
        self._clock = clock
        self._owner_for = owner_for
        self._next_sequence = next_sequence
        self._origin_sequence = origin_sequence

    async def __call__(self, event: OutboundEvent) -> None:
        conversation_key = getattr(event, "conversation_key", "")
        if not conversation_key:
            return
        owner = await self._owner_for(conversation_key)
        if owner is None:
            return
        sequence = await self._next_sequence(conversation_key)
        origin_seq = await self._origin_sequence(event) if self._origin_sequence else None
        payload = {"kind": type(event).__name__, "event": _json_value(asdict(event))}
        await self._writer.put_sanitized(
            output_id=f"{conversation_key}:{sequence}",
            conversation_key=conversation_key,
            conversation_generation=owner.conversation_generation,
            sequence=sequence,
            origin_seq=origin_seq,
            owner=owner,
            payload=payload,
            now=self._clock.now(),
            created_at=self._clock.now(),
        )


def _json_value(value: Any) -> Any:
    if is_dataclass(value):
        return _json_value(asdict(value))
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value
