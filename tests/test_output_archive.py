from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta

import pytest

from silk_agent.core.distributed_models import CommandExecutionState
from silk_agent.sandbox.output_archive import (
    CommandOutputArchiver,
    GCSCommandOutputStore,
    OutputArchiveError,
    OutputArchiveExpired,
    OutputPage,
)
from silk_agent.stores.firestore.command_monitor import FirestoreCommandMonitorStore


class Clock:
    def __init__(self, now: datetime) -> None:
        self.value = now

    def now(self) -> datetime:
        return self.value


class PagedReader:
    def __init__(self, streams: dict[str, bytes], page_size: int) -> None:
        self.streams = streams
        self.page_size = page_size
        self.calls: list[tuple[str, int]] = []

    async def read_output_page(
        self, _key, _command_id, *, stream, cursor, limit_bytes
    ) -> OutputPage:
        self.calls.append((stream, cursor))
        assert limit_bytes == self.page_size
        data = self.streams[stream][cursor : cursor + limit_bytes]
        next_cursor = cursor + len(data)
        return OutputPage(data, next_cursor, next_cursor >= len(self.streams[stream]))


class MemoryObjects:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put(
        self,
        *,
        command_id,
        sandbox_generation,
        stream,
        source,
        size_bytes,
        sha256,
        expires_at,
    ) -> str:
        uri = f"gs://test/{sandbox_generation}/{command_id}/{stream}"
        data = source.read()
        assert len(data) == size_bytes
        self.objects[uri] = data
        return uri


class FakeBlob:
    def __init__(self) -> None:
        self.metadata = None
        self.data = b""
        self.size = 0

    def upload_from_file(self, source, **_kwargs) -> None:
        if self.data:
            raise RuntimeError("object already exists")
        self.data = source.read()
        self.size = len(self.data)

    def reload(self) -> None:
        return None

    def download_as_bytes(self, *, start, end) -> bytes:
        return self.data[start : end + 1]


class FakeBucket:
    def __init__(self) -> None:
        self.blobs: dict[str, FakeBlob] = {}

    def blob(self, name: str) -> FakeBlob:
        return self.blobs.setdefault(name, FakeBlob())

    def get_blob(self, name: str) -> FakeBlob | None:
        return self.blobs.get(name)


class FakeStorageClient:
    def __init__(self) -> None:
        self.value = FakeBucket()

    def bucket(self, _name: str) -> FakeBucket:
        return self.value


async def test_archiver_pages_output_and_keeps_only_bounded_tail_in_metadata():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    clock = Clock(now)
    objects = MemoryObjects()
    reader = PagedReader({"stdout": b"abcdefghij", "stderr": b"warning"}, page_size=4)
    archive = CommandOutputArchiver(
        objects, clock=clock, retention_days=2, page_bytes=4, tail_bytes=5
    )

    result = await archive.archive(
        reader,
        conversation_key="conversation",
        command_id="command-1",
        sandbox_generation=8,
    )

    assert reader.calls == [
        ("stdout", 0),
        ("stdout", 4),
        ("stdout", 8),
        ("stderr", 0),
        ("stderr", 4),
    ]
    stdout = result["stdout"]
    assert stdout["size_bytes"] == 10
    assert stdout["tail"] == "fghij"
    assert stdout["expires_at"] == (now + timedelta(days=2)).isoformat()
    assert objects.objects[stdout["uri"]] == b"abcdefghij"


async def test_archiver_rejects_a_stalled_cursor_instead_of_saving_partial_success():
    class StalledReader:
        async def read_output_page(self, *_args, **_kwargs):
            return OutputPage(b"", 0, False)

    archive = CommandOutputArchiver(MemoryObjects(), clock=Clock(datetime.now(UTC)))
    with pytest.raises(OutputArchiveError, match="stopped before"):
        await archive.archive(
            StalledReader(),
            conversation_key="conversation",
            command_id="command-1",
            sandbox_generation=1,
        )


async def test_gcs_output_store_supports_byte_cursor_and_expiry():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    client = FakeStorageClient()
    store = GCSCommandOutputStore("test", client=client)  # type: ignore[arg-type]
    uri = await store.put(
        command_id="command-1",
        sandbox_generation=2,
        stream="stdout",
        source=io.BytesIO(b"0123456789"),
        size_bytes=10,
        sha256="abc",
        expires_at=now + timedelta(days=1),
    )

    first = await store.read(uri, cursor=0, limit_bytes=4, now=now)
    second = await store.read(uri, cursor=first[1], limit_bytes=4, now=now)
    third = await store.read(uri, cursor=second[1], limit_bytes=4, now=now)
    assert first == (b"0123", 4, False)
    assert second == (b"4567", 8, False)
    assert third == (b"89", 10, True)
    with pytest.raises(OutputArchiveExpired):
        await store.read(uri, cursor=0, limit_bytes=4, now=now + timedelta(days=1))


async def test_firestore_terminal_metadata_rejects_full_output_payloads():
    store = object.__new__(FirestoreCommandMonitorStore)
    with pytest.raises(ValueError, match="size budget"):
        await store.observe_terminal(
            "command-1",
            claim_token="claim",
            expected=CommandExecutionState.RUNNING,
            terminal=CommandExecutionState.FINISHED,
            result={"stdout": "x" * (65 * 1024)},
            now=datetime(2026, 10, 14, tzinfo=UTC),
        )
