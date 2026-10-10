"""Bot start-up and shutdown handling for turns that did not finish."""

from __future__ import annotations

import logging
import uuid

from silk_agent.core.events import TurnInterrupted
from silk_agent.core.ports import ChatChannel, Clock, SessionStateStore

log = logging.getLogger(__name__)

DEFAULT_SHUTDOWN_GRACE_SECONDS = 25.0


def new_bot_instance_id() -> str:
    """A fresh id for every Bot process; recorded with each running turn."""
    return uuid.uuid4().hex


async def recover_interrupted(
    store: SessionStateStore, channel: ChatChannel, bot_instance_id: str, clock: Clock
) -> list[str]:
    """Called once at start-up. Every conversation that is "running" under a different
    instance id lost its Bot (crash / kill): mark it interrupted and tell the channel.

    The turn is NOT restarted; the user resumes it explicitly."""
    recovered: list[str] = []
    for state in await store.list_running():
        if state.bot_instance_id == bot_instance_id:
            continue
        state.status = "interrupted_crash"
        state.updated_at = clock.now()
        previous_turn = state.turn_id or ""
        try:
            await store.put(state)
        except Exception:
            log.exception("could not mark %s as interrupted", state.conversation_key)
            continue
        await channel.send(
            TurnInterrupted(state.conversation_key, previous_turn, "crash", state.updated_at)
        )
        recovered.append(state.conversation_key)
    return recovered
