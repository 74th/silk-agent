"""Generic defaults bundled with silk-agent (system prompt, input guard policy)."""

from __future__ import annotations

from importlib.resources import files


def read_resource(name: str) -> str:
    return files(__name__).joinpath(name).read_text(encoding="utf-8")
