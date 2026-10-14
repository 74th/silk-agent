"""Cursor based command output archival with bounded Firestore metadata."""

from __future__ import annotations

import asyncio
import hashlib
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import BinaryIO, Protocol, cast, runtime_checkable

from google.cloud import storage

from silk_agent.stores.firestore.ids import document_id

OUTPUT_TAIL_BYTES = 3072
DEFAULT_PAGE_BYTES = 256 * 1024


@dataclass(frozen=True)
class OutputPage:
    data: bytes
    next_cursor: int
    complete: bool


@runtime_checkable
class OutputPageReader(Protocol):
    async def read_output_page(
        self,
        conversation_key: str,
        command_id: str,
        *,
        stream: str,
        cursor: int,
        limit_bytes: int,
    ) -> OutputPage: ...


class Clock(Protocol):
    def now(self) -> datetime: ...


class OutputObjectStore(Protocol):
    async def put(
        self,
        *,
        command_id: str,
        sandbox_generation: int,
        stream: str,
        source: BinaryIO,
        size_bytes: int,
        sha256: str,
        expires_at: datetime,
    ) -> str: ...


class OutputObjectReader(Protocol):
    async def read(
        self, uri: str, *, cursor: int, limit_bytes: int, now: datetime
    ) -> tuple[bytes, int, bool]: ...


class OutputArchiveError(RuntimeError):
    """Output could not be read or durably archived."""


class OutputArchiveExpired(OutputArchiveError):
    """An archived output object has expired and must not be returned as complete."""


class CommandOutputArchiver:
    def __init__(
        self,
        objects: OutputObjectStore,
        *,
        clock: Clock,
        retention_days: int = 30,
        page_bytes: int = DEFAULT_PAGE_BYTES,
        tail_bytes: int = OUTPUT_TAIL_BYTES,
    ) -> None:
        if retention_days <= 0 or page_bytes <= 0 or tail_bytes <= 0:
            raise ValueError("retention_days, page_bytes, and tail_bytes must be positive")
        self._objects = objects
        self._clock = clock
        self._retention = timedelta(days=retention_days)
        self._page_bytes = page_bytes
        self._tail_bytes = tail_bytes

    async def archive(
        self,
        reader: OutputPageReader,
        *,
        conversation_key: str,
        command_id: str,
        sandbox_generation: int,
    ) -> Mapping[str, object]:
        expires_at = self._clock.now() + self._retention
        archived: dict[str, object] = {}
        for stream in ("stdout", "stderr"):
            archived[stream] = await self._archive_stream(
                reader,
                conversation_key=conversation_key,
                command_id=command_id,
                sandbox_generation=sandbox_generation,
                stream=stream,
                expires_at=expires_at,
            )
        return archived

    async def _archive_stream(
        self,
        reader: OutputPageReader,
        *,
        conversation_key: str,
        command_id: str,
        sandbox_generation: int,
        stream: str,
        expires_at: datetime,
    ) -> Mapping[str, object]:
        cursor = 0
        size_bytes = 0
        digest = hashlib.sha256()
        tail = bytearray()
        with tempfile.SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b") as output:
            while True:
                page = await reader.read_output_page(
                    conversation_key,
                    command_id,
                    stream=stream,
                    cursor=cursor,
                    limit_bytes=self._page_bytes,
                )
                if page.next_cursor < cursor or page.next_cursor - cursor != len(page.data):
                    raise OutputArchiveError("Sandbox output cursor did not match returned bytes")
                if not page.data and not page.complete:
                    raise OutputArchiveError(
                        "Sandbox output stopped before the stream was complete"
                    )
                if page.data:
                    await asyncio.to_thread(output.write, page.data)
                    digest.update(page.data)
                    size_bytes += len(page.data)
                    tail.extend(page.data)
                    if len(tail) > self._tail_bytes:
                        del tail[: len(tail) - self._tail_bytes]
                cursor = page.next_cursor
                if page.complete:
                    break
            output.seek(0)
            uri = await self._objects.put(
                command_id=command_id,
                sandbox_generation=sandbox_generation,
                stream=stream,
                source=cast(BinaryIO, output),
                size_bytes=size_bytes,
                sha256=digest.hexdigest(),
                expires_at=expires_at,
            )
        return {
            "uri": uri,
            "size_bytes": size_bytes,
            "sha256": digest.hexdigest(),
            "expires_at": expires_at.isoformat(),
            "tail": bytes(tail).decode("utf-8", errors="replace"),
        }


class GCSCommandOutputStore:
    """Cloud Storage archive; immutable, deterministic names make retries idempotent."""

    def __init__(self, bucket: str, *, client: storage.Client | None = None) -> None:
        if not bucket or bucket.strip() != bucket:
            raise ValueError("bucket must not be empty")
        self.bucket_name = bucket
        self._client = client or storage.Client()

    async def put(
        self,
        *,
        command_id: str,
        sandbox_generation: int,
        stream: str,
        source: BinaryIO,
        size_bytes: int,
        sha256: str,
        expires_at: datetime,
    ) -> str:
        if stream not in {"stdout", "stderr"} or sandbox_generation < 1:
            raise ValueError("invalid command output identity")
        if size_bytes < 0 or not sha256:
            raise ValueError("invalid output metadata")
        _validate_utc(expires_at, "expires_at")
        object_name = (
            f"command-output/{sandbox_generation}/"
            f"{document_id(command_id, prefix='command')}/{stream}"
        )
        blob = self._client.bucket(self.bucket_name).blob(object_name)
        blob.metadata = {"sha256": sha256, "expires_at": expires_at.isoformat()}

        def upload() -> None:
            source.seek(0)
            try:
                blob.upload_from_file(
                    source,
                    rewind=True,
                    content_type="application/octet-stream",
                    if_generation_match=0,
                    checksum="auto",
                )
            except Exception:
                # A lost response may follow a successful create. Accept only an object
                # with the exact digest we attempted to save.
                existing = self._client.bucket(self.bucket_name).get_blob(object_name)
                if existing is None or (existing.metadata or {}).get("sha256") != sha256:
                    raise

        try:
            await asyncio.to_thread(upload)
        except Exception as exc:
            raise OutputArchiveError("Cloud Storage output upload failed") from exc
        return f"gs://{self.bucket_name}/{object_name}"

    async def read(
        self,
        uri: str,
        *,
        cursor: int,
        limit_bytes: int,
        now: datetime,
    ) -> tuple[bytes, int, bool]:
        _validate_utc(now, "now")
        if cursor < 0 or limit_bytes <= 0:
            raise ValueError("cursor must be nonnegative and limit_bytes must be positive")
        prefix = f"gs://{self.bucket_name}/"
        if not uri.startswith(prefix):
            raise ValueError("output reference is outside the configured bucket")
        blob = self._client.bucket(self.bucket_name).blob(uri.removeprefix(prefix))

        def download() -> tuple[bytes, int, bool]:
            blob.reload()
            metadata = blob.metadata or {}
            expiry = datetime.fromisoformat(metadata.get("expires_at", ""))
            if expiry.tzinfo is None or expiry <= now:
                raise OutputArchiveExpired("command output archive has expired")
            total = int(blob.size or 0)
            if cursor > total:
                raise ValueError("cursor exceeds output size")
            end = min(total, cursor + limit_bytes)
            data = blob.download_as_bytes(start=cursor, end=end - 1) if end > cursor else b""
            next_cursor = cursor + len(data)
            return data, next_cursor, next_cursor >= total

        try:
            return await asyncio.to_thread(download)
        except OutputArchiveExpired:
            raise
        except Exception as exc:
            raise OutputArchiveError("Cloud Storage output read failed") from exc


def _validate_utc(value: datetime, field_name: str) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be UTC-aware")
