"""Process-local conversation state (used when no durable store is configured)."""

from __future__ import annotations

from silk_agent.core.ports import ConversationState


class InMemoryStateStore:
    def __init__(self) -> None:
        self.states: dict[str, ConversationState] = {}

    async def get(self, conversation_key: str) -> ConversationState | None:
        s = self.states.get(conversation_key)
        return None if s is None else _copy(s)

    async def put(self, state: ConversationState) -> None:
        self.states[state.conversation_key] = _copy(state)

    async def list_running(self) -> list[ConversationState]:
        return [_copy(s) for s in self.states.values() if s.status == "running"]


def _copy(s: ConversationState) -> ConversationState:
    return ConversationState(
        s.conversation_key,
        s.agent_session_id,
        s.status,
        s.turn_id,
        s.bot_instance_id,
        dict(s.progress_ref),
        list(s.running_command_ids),
        s.updated_at,
        s.owner_user,
        s.owner_email,
    )
