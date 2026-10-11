from __future__ import annotations

import pytest

from gke_session_sandbox import watch_with_relist


class ApiError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status


@pytest.mark.asyncio
async def test_expired_watch_resource_version_triggers_list_recovery() -> None:
    list_versions = iter(("10", "25"))
    watch_calls: list[str] = []

    async def list_objects() -> dict:
        return {"metadata": {"resourceVersion": next(list_versions)}, "items": []}

    async def watch_events(resource_version: str):
        watch_calls.append(resource_version)
        if len(watch_calls) == 1:
            raise ApiError(410)
        yield {"type": "ADDED", "object": {"metadata": {"resourceVersion": "26"}}}

    events = [event async for event in watch_with_relist(list_objects, watch_events)]

    assert [event["type"] for event in events] == ["RELIST", "RELIST", "ADDED"]
    assert watch_calls == ["10", "25"]


@pytest.mark.asyncio
async def test_watch_errors_other_than_410_propagate() -> None:
    async def list_objects() -> dict:
        return {"metadata": {"resourceVersion": "1"}, "items": []}

    async def watch_events(resource_version: str):
        raise ApiError(403)
        yield  # pragma: no cover - make this an async generator

    with pytest.raises(ApiError) as exc_info:
        _ = [event async for event in watch_with_relist(list_objects, watch_events)]
    assert exc_info.value.status == 403
