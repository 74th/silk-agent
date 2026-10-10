"""Skill sources. Every sync produces an immutable *generation* directory:

    <cache>/<generation_id>/.claude/skills/<skill>/SKILL.md ...

A turn pins one generation, so a sync in the middle of a turn never changes what the
running turn sees; the next turn picks up the new generation.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from silk_agent.core.ports import Clock, SkillGeneration

log = logging.getLogger(__name__)

KEEP_GENERATIONS = 5


def _digest(items: Iterable[tuple[str, str]]) -> str:
    h = hashlib.sha256()
    for name, version in sorted(items):
        h.update(f"{name}\0{version}\n".encode())
    return h.hexdigest()[:16]


def link_skills(cwd: Path, generation: SkillGeneration | None) -> None:
    """Point `<cwd>/.claude/skills` at the generation (or remove it)."""
    link = cwd / ".claude" / "skills"
    if link.is_symlink() or link.exists():
        if link.is_symlink() or link.is_file():
            link.unlink()
        else:
            shutil.rmtree(link)
    if generation is None:
        return
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(generation.root / ".claude" / "skills", target_is_directory=True)


class _CachedSource:
    def __init__(self, cache_dir: str | Path) -> None:
        self._cache = Path(cache_dir)
        self._current: SkillGeneration | None = None

    def current(self) -> SkillGeneration | None:
        return self._current

    def _adopt(self, generation_id: str, build: Any) -> SkillGeneration:
        root = self._cache / generation_id
        if not root.exists():
            tmp = self._cache / f".tmp-{generation_id}"
            if tmp.exists():
                shutil.rmtree(tmp)
            (tmp / ".claude" / "skills").mkdir(parents=True)
            try:
                build(tmp / ".claude" / "skills")
            except BaseException:
                shutil.rmtree(tmp, ignore_errors=True)
                raise
            tmp.rename(root)
        self._current = SkillGeneration(generation_id, root)
        self._prune()
        return self._current

    def _prune(self) -> None:
        if self._current is None or not self._cache.exists():
            return
        dirs = sorted(
            (d for d in self._cache.iterdir() if d.is_dir() and not d.name.startswith(".")),
            key=lambda d: d.stat().st_mtime,
            reverse=True,
        )
        for old in dirs[KEEP_GENERATIONS:]:
            if old.name != self._current.generation_id:
                shutil.rmtree(old, ignore_errors=True)


class LocalDirSkillSource(_CachedSource):
    """`source_dir/<skill>/SKILL.md ...` on the local disk."""

    def __init__(self, source_dir: str | Path, cache_dir: str | Path) -> None:
        super().__init__(cache_dir)
        self._src = Path(source_dir)

    def _sync(self) -> SkillGeneration | None:
        if not self._src.is_dir():
            return None
        files = [p for p in self._src.rglob("*") if p.is_file()]
        if not files:
            return None
        gen_id = _digest(
            (str(p.relative_to(self._src)), f"{p.stat().st_size}:{p.stat().st_mtime_ns}")
            for p in files
        )
        if self._current is not None and self._current.generation_id == gen_id:
            return self._current

        def build(dest: Path) -> None:
            shutil.copytree(self._src, dest, dirs_exist_ok=True)

        return self._adopt(gen_id, build)

    async def refresh(self) -> SkillGeneration | None:
        return await asyncio.to_thread(self._sync)


class GcsSkillSource(_CachedSource):
    """`gs://<bucket>/<prefix>/<skill>/SKILL.md ...`. The generation id is a hash of the
    objects' names and GCS generation numbers."""

    def __init__(
        self,
        source: str,
        cache_dir: str | Path,
        *,
        client: Any | None = None,
    ) -> None:
        super().__init__(cache_dir)
        if not source.startswith("gs://"):
            raise ValueError(f"not a gs:// URL: {source}")
        bucket, _, prefix = source[len("gs://") :].partition("/")
        self._bucket = bucket
        self._prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self._client = client

    def _get_client(self) -> Any:
        if self._client is None:
            from google.cloud import storage

            self._client = storage.Client()
        return self._client

    def _sync(self) -> SkillGeneration | None:
        blobs = [
            b
            for b in self._get_client().list_blobs(self._bucket, prefix=self._prefix)
            if not b.name.endswith("/")
        ]
        if not blobs:
            return None
        gen_id = _digest((b.name, str(b.generation)) for b in blobs)
        if self._current is not None and self._current.generation_id == gen_id:
            return self._current

        def build(dest: Path) -> None:
            for blob in blobs:
                target = dest / blob.name[len(self._prefix) :]
                target.parent.mkdir(parents=True, exist_ok=True)
                blob.download_to_filename(str(target))

        return self._adopt(gen_id, build)

    async def refresh(self) -> SkillGeneration | None:
        return await asyncio.to_thread(self._sync)


async def run_skill_sync(
    source: Any, clock: Clock, interval_seconds: float, *, stop: asyncio.Event | None = None
) -> None:
    """Refresh periodically. A failed sync keeps the previous generation."""
    while stop is None or not stop.is_set():
        try:
            await source.refresh()
        except Exception:
            log.exception("skill sync failed; keeping the previous generation")
        await clock.sleep(interval_seconds)
