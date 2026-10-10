"""Load MCP servers from the configured file only (HTTP / SSE; `${ENV}` expansion)."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

ALLOWED_TYPES = {"http", "sse"}
_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class McpConfigError(ValueError):
    pass


def _expand(value: Any, env: Mapping[str, str], where: str) -> Any:
    if isinstance(value, str):

        def sub(m: re.Match[str]) -> str:
            name = m.group(1)
            if name not in env:
                raise McpConfigError(f"{where}: environment variable {name} is not set")
            return env[name]

        return _ENV.sub(sub, value)
    if isinstance(value, dict):
        return {k: _expand(v, env, where) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v, env, where) for v in value]
    return value


def load_mcp_servers(
    path: str | Path | None, env: Mapping[str, str] | None = None
) -> dict[str, dict[str, Any]]:
    """Return `{name: server config}` for ClaudeAgentOptions.mcp_servers."""
    if path is None:
        return {}
    env = os.environ if env is None else env
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    servers = data.get("mcpServers", data)
    result: dict[str, dict[str, Any]] = {}
    for name, cfg in servers.items():
        if name == "sandbox":
            raise McpConfigError("the name 'sandbox' is reserved for the built-in sandbox tools")
        kind = cfg.get("type", "stdio" if "command" in cfg else "http")
        if kind not in ALLOWED_TYPES:
            raise McpConfigError(
                f"{name}: transport '{kind}' is not allowed (only {sorted(ALLOWED_TYPES)})"
            )
        if "url" not in cfg:
            raise McpConfigError(f"{name}: 'url' is required")
        result[name] = _expand({**cfg, "type": kind}, env, name)
    return result
