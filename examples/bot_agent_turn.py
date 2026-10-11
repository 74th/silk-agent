"""Small Bot adapter example with durable context-event delivery."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from gke_session_sandbox import CommandResult, SandboxManager, SessionKey


class ConversationStore(Protocol):
    async def append_once(self, event_id: str, message: str) -> None:
        """Durably append a message unless this event_id was already stored."""


async def run_agent_turn(
    manager: SandboxManager,
    conversation_store: ConversationStore,
    key: SessionKey,
    prompt: str,
    *,
    agent_argv: Sequence[str],
) -> CommandResult:
    """Deliver pending context, then run exactly one tracked agent turn."""
    session = await manager.ensure(key)
    for event in session.pending_context_events:
        await conversation_store.append_once(event.event_id, event.context_message)
        await manager.ack_context_event(key, event.event_id)
    async with session.commands as commands:
        return await commands.run(
            [*agent_argv, prompt],
            check=True,
            timeout_seconds=600,
        )
