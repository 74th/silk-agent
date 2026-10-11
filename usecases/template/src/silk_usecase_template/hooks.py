"""Example hooks for audit logging.

Configured in config/silk-agent.yaml as `hooks: ["silk_usecase_template.hooks:create"]`.

Hooks receive the raw content (user input, tool input and result, the agent's reply before the
output check). Nothing is masked for you: decide here what may leave the process. `mask()` below
shows one way (it hides e-mail addresses and `key=value` secrets). Delivery is asynchronous and
observation-only: an exception or a slow call here never affects a conversation, but events are
dropped when the queue overflows, so use a durable queue (Pub/Sub, ...) if you must not lose any.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import asdict
from typing import Any

from silk_agent.hooks import (
    AgentReply,
    BaseHooks,
    HookContext,
    HookEvent,
    Hooks,
    SystemNotice,
    ToolEnd,
    ToolStart,
    TurnEnd,
    TurnStart,
    UserInput,
)

_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_SECRET = re.compile(r"(?i)\b(api[_-]?key|token|secret|password)(\s*[=:]\s*)\S+")


def mask(value: Any) -> Any:
    """Hide e-mail addresses and `key=value` secrets in strings (also inside dicts / lists)."""
    if isinstance(value, str):
        return _SECRET.sub(r"\1\2[MASKED]", _EMAIL.sub("[EMAIL]", value))
    if isinstance(value, dict):
        return {k: mask(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [mask(v) for v in value]
    return value


class AuditHooks(BaseHooks):
    """Writes every event as one masked JSON line on stdout (Cloud Logging collects it).

    Replace `_write` to send events somewhere else (BigQuery, Pub/Sub, a webhook, ...)."""

    def __init__(self, *, include_content: bool) -> None:
        self._include_content = include_content

    async def on_user_input(self, event: UserInput) -> None:
        self._write(event)

    async def on_tool_start(self, event: ToolStart) -> None:
        self._write(event)

    async def on_tool_end(self, event: ToolEnd) -> None:
        self._write(event)

    async def on_agent_reply(self, event: AgentReply) -> None:
        self._write(event)

    async def on_system_notice(self, event: SystemNotice) -> None:
        self._write(event)

    # Turn boundaries are cheap and carry no content: keep them as well.
    async def on_turn_start(self, event: TurnStart) -> None:
        self._write(event)

    async def on_turn_end(self, event: TurnEnd) -> None:
        self._write(event)

    def _write(self, event: HookEvent) -> None:
        data = asdict(event)
        for name in event.CONTENT_FIELDS:
            if self._include_content:
                data[name] = mask(data[name])
            else:
                data.pop(name, None)
        data["at"] = event.at.isoformat()
        sys.stdout.write(json.dumps(data, ensure_ascii=False, default=str) + "\n")
        sys.stdout.flush()


def create(ctx: HookContext) -> Hooks:
    """The factory named in the config. `options` come from the config's `hooks` entry."""
    return AuditHooks(include_content=bool(ctx.options.get("include_content", False)))
