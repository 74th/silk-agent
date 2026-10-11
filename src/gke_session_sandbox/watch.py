"""Kubernetes list/watch recovery helpers for expired resource versions."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Mapping
from typing import Any


async def watch_with_relist(
    list_objects: Callable[[], Awaitable[Any]],
    watch_events: Callable[[str], AsyncIterable[Mapping[str, Any]]],
) -> AsyncIterator[Mapping[str, Any]]:
    """Yield one relist snapshot, then watch; relist and resume after HTTP 410.

    A normal watch stream end returns control to the caller so its supervisor can
    reconnect according to its backoff policy.
    """
    snapshot = await list_objects()
    yield {"type": "RELIST", "object": snapshot}
    resource_version = _resource_version(snapshot)
    while True:
        try:
            async for event in watch_events(resource_version):
                yield event
                resource_version = _event_resource_version(event) or resource_version
        except Exception as exc:
            if getattr(exc, "status", None) != 410:
                raise
            snapshot = await list_objects()
            yield {"type": "RELIST", "object": snapshot}
            resource_version = _resource_version(snapshot)
            continue
        return


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    snake = "".join(("_" + char.lower()) if char.isupper() else char for char in name).lstrip("_")
    return getattr(value, snake, None)


def _resource_version(value: Any) -> str:
    metadata = _field(value, "metadata")
    version = _field(metadata, "resourceVersion")
    if not version:
        raise ValueError("Kubernetes list response has no metadata.resourceVersion")
    return str(version)


def _event_resource_version(event: Mapping[str, Any]) -> str | None:
    return _resource_version(event.get("object")) if _field(event, "object") is not None else None
