"""Display names and short summaries for tool calls."""

from __future__ import annotations

from typing import Any

# Tools provided by the built-in sandbox MCP server are shown by their bare name.
BUILTIN_SERVERS = {"sandbox"}
SUMMARY_MAX = 120


def display_name(tool_name: str) -> str:
    """`mcp__docs__search` -> `docs/search`, `mcp__sandbox__bash` -> `bash`."""
    if tool_name.startswith("mcp__"):
        _, _, rest = tool_name.partition("__")
        server, _, tool = rest.partition("__")
        if tool:
            return tool if server in BUILTIN_SERVERS else f"{server}/{tool}"
    return tool_name


def _first_line(text: str) -> str:
    return text.strip().splitlines()[0] if text.strip() else ""


def _clip(text: str, limit: int = SUMMARY_MAX) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def summarize(tool_name: str, tool_input: dict[str, Any]) -> str:
    name = display_name(tool_name)
    if name == "bash":
        return _clip(_first_line(str(tool_input.get("command", ""))))
    for key in ("path", "file_path"):
        if name in ("read_file", "write_file", "edit_file", "list_files") and key in tool_input:
            return _clip(str(tool_input[key]))
    if name in ("glob", "grep"):
        pattern = str(tool_input.get("pattern", ""))
        path = tool_input.get("path")
        return _clip(f"{pattern} {path}" if path else pattern)
    if name in ("command_status", "command_output"):
        return _clip(str(tool_input.get("command_id", "")))
    # Other (MCP) tools: first short scalar arguments.
    parts = [
        f"{k}={v}"
        for k, v in tool_input.items()
        if isinstance(v, str | int | float | bool) and len(str(v)) <= 60
    ][:3]
    return _clip(" ".join(parts))
