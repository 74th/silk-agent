"""Translate sanitized Outbox events to the Slack presentation adapter."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from silk_agent.channels.slack_output import SlackOutput
from silk_agent.core.events import (
    AssistantText,
    GuardBlocked,
    Heartbeat,
    MessageAppended,
    Notice,
    StatusChanged,
    ToolFinished,
    ToolStarted,
    TurnFailed,
    TurnFinished,
    TurnInterrupted,
    TurnStarted,
    Usage,
)

_EVENTS = {
    event.__name__: event
    for event in (
        AssistantText,
        GuardBlocked,
        Heartbeat,
        MessageAppended,
        Notice,
        StatusChanged,
        ToolFinished,
        ToolStarted,
        TurnFailed,
        TurnFinished,
        TurnInterrupted,
        TurnStarted,
    )
}


class SlackOutboxDelivery:
    """Callable adapter for ``OutboxSender``; malformed payloads fail closed."""

    def __init__(self, output: SlackOutput) -> None:
        self._output = output

    async def __call__(self, record: Mapping[str, Any]) -> Mapping[str, str] | None:
        payload = record.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("Outbox payload must be a mapping")
        kind = payload.get("kind")
        event_data = payload.get("event")
        if not isinstance(kind, str) or not isinstance(event_data, Mapping):
            raise ValueError("Outbox event is malformed")
        event_type = _EVENTS.get(kind)
        if event_type is None:
            raise ValueError("Outbox event kind is unsupported")
        values = dict(event_data)
        for field in ("started_at", "finished_at", "at"):
            value = values.get(field)
            if isinstance(value, str):
                values[field] = datetime.fromisoformat(value)
        if kind == "TurnFinished":
            usage = values.get("usage", {})
            if not isinstance(usage, Mapping):
                raise ValueError("Outbox usage is malformed")
            values["usage"] = Usage(**usage)
            values["warnings"] = tuple(values.get("warnings", ()))
        event = event_type(**values)
        await self._output.send_durable(event)
        return None
