import asyncio
from pathlib import Path

import pytest

from silk_agent.core.ports import SkillGeneration
from silk_agent.skills.sources import (
    GcsSkillSource,
    LocalDirSkillSource,
    link_skills,
    run_skill_sync,
)


def write_skill(root: Path, name: str, body: str):
    d = root / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "SKILL.md").write_text(body)
    (d / "run.sh").write_text("echo hi")


class FakeBlob:
    def __init__(self, name, generation, content):
        self.name, self.generation, self.content = name, generation, content

    def download_to_filename(self, path):
        Path(path).write_text(self.content)


class FakeGcs:
    def __init__(self):
        self.blobs: list[FakeBlob] = []
        self.listed = 0

    def list_blobs(self, bucket, prefix=""):
        self.listed += 1
        assert bucket == "bkt"
        return [b for b in self.blobs if b.name.startswith(prefix)]


async def test_local_source_generation_changes_on_update(tmp_path):
    src, cache = tmp_path / "src", tmp_path / "cache"
    write_skill(src, "report", "v1")
    s = LocalDirSkillSource(src, cache)
    g1 = await s.refresh()
    assert g1 and (g1.root / ".claude/skills/report/SKILL.md").read_text() == "v1"
    assert (await s.refresh()) == g1  # unchanged
    (src / "report" / "SKILL.md").write_text("version two")
    g2 = await s.refresh()
    assert g2 and g2.generation_id != g1.generation_id
    assert (g2.root / ".claude/skills/report/SKILL.md").read_text() == "version two"
    # the old generation is untouched, so a running turn keeps its view
    assert (g1.root / ".claude/skills/report/SKILL.md").read_text() == "v1"


async def test_empty_source_has_no_generation(tmp_path):
    s = LocalDirSkillSource(tmp_path / "missing", tmp_path / "cache")
    assert await s.refresh() is None and s.current() is None


async def test_gcs_source_downloads_and_detects_updates(tmp_path):
    gcs = FakeGcs()
    gcs.blobs = [
        FakeBlob("skills/report/SKILL.md", 1, "v1"),
        FakeBlob("skills/report/run.sh", 1, "echo"),
        FakeBlob("other/ignored.txt", 1, "x"),
    ]
    s = GcsSkillSource("gs://bkt/skills", tmp_path / "cache", client=gcs)
    g1 = await s.refresh()
    assert g1 is not None
    assert (g1.root / ".claude/skills/report/SKILL.md").read_text() == "v1"
    assert not (g1.root / ".claude/skills/ignored.txt").exists()
    assert (await s.refresh()).generation_id == g1.generation_id
    gcs.blobs[0] = FakeBlob("skills/report/SKILL.md", 2, "v2")
    g2 = await s.refresh()
    assert g2 and g2.generation_id != g1.generation_id
    assert (g2.root / ".claude/skills/report/SKILL.md").read_text() == "v2"
    assert s.current() == g2


async def test_sync_failure_keeps_previous_generation(tmp_path):
    gcs = FakeGcs()
    gcs.blobs = [FakeBlob("skills/a/SKILL.md", 1, "x")]
    s = GcsSkillSource("gs://bkt/skills", tmp_path / "cache", client=gcs)
    g1 = await s.refresh()

    def boom(*a, **k):
        raise RuntimeError("gcs down")

    gcs.list_blobs = boom
    try:
        await s.refresh()
    except RuntimeError:
        pass
    assert s.current() == g1


async def test_periodic_sync_and_failure_tolerance(clock):
    calls = []

    class Src:
        async def refresh(self):
            calls.append(clock.now())
            if len(calls) == 2:
                raise RuntimeError("transient")

    stop = asyncio.Event()
    task = asyncio.create_task(run_skill_sync(Src(), clock, 300, stop=stop))
    await clock.advance(0)
    await clock.advance(300)
    await clock.advance(300)
    assert len(calls) == 3  # the failure at the 2nd call did not stop the loop
    stop.set()
    await clock.advance(300)
    await task


def test_link_skills_points_cwd_to_generation(tmp_path):
    gen_root = tmp_path / "gen1"
    (gen_root / ".claude/skills/a").mkdir(parents=True)
    (gen_root / ".claude/skills/a/SKILL.md").write_text("x")
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    link_skills(cwd, SkillGeneration("g1", gen_root))
    assert (cwd / ".claude/skills/a/SKILL.md").read_text() == "x"
    gen2 = tmp_path / "gen2"
    (gen2 / ".claude/skills/b").mkdir(parents=True)
    link_skills(cwd, SkillGeneration("g2", gen2))
    assert (cwd / ".claude/skills").resolve() == (gen2 / ".claude/skills").resolve()
    link_skills(cwd, None)
    assert not (cwd / ".claude/skills").exists()


async def test_gcs_source_rejects_an_object_name_that_escapes_the_generation(tmp_path):
    gcs = FakeGcs()
    gcs.blobs = [FakeBlob("skills/../../escaped.txt", 1, "x")]
    s = GcsSkillSource("gs://bkt/skills", tmp_path / "cache", client=gcs)
    with pytest.raises(ValueError, match="escapes"):
        await s.refresh()
    assert not (tmp_path / "escaped.txt").exists()


async def test_emptying_the_source_drops_the_old_generation(tmp_path):
    src, cache = tmp_path / "src", tmp_path / "cache"
    write_skill(src, "report", "v1")
    s = LocalDirSkillSource(src, cache)
    assert await s.refresh() is not None
    for path in sorted(src.rglob("*"), reverse=True):
        path.unlink() if path.is_file() else path.rmdir()
    assert await s.refresh() is None and s.current() is None
