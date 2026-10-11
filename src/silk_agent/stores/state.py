"""SessionStateStore on Agent Platform Sessions (append-only control log per conversation)."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from silk_agent.core.ports import ConversationState
from silk_agent.stores.agent_platform import AgentPlatformSessions, control_session_id

log = logging.getLogger(__name__)

CONTROL_USER_ID = "silk-agent-control"


def state_to_dict(state: ConversationState) -> dict[str, Any]:
    return {
        "conversation_key": state.conversation_key,
        "agent_session_id": state.agent_session_id,
        "status": state.status,
        "turn_id": state.turn_id,
        "bot_instance_id": state.bot_instance_id,
        "progress_ref": state.progress_ref,
        "running_command_ids": state.running_command_ids,
        "updated_at": state.updated_at.isoformat() if state.updated_at else None,
        "owner_user": state.owner_user,
        "owner_email": state.owner_email,
    }


def state_from_dict(data: dict[str, Any]) -> ConversationState:
    updated = data.get("updated_at")
    return ConversationState(
        conversation_key=data["conversation_key"],
        agent_session_id=data.get("agent_session_id"),
        status=data.get("status", "idle"),
        turn_id=data.get("turn_id"),
        bot_instance_id=data.get("bot_instance_id"),
        progress_ref=dict(data.get("progress_ref") or {}),
        running_command_ids=list(data.get("running_command_ids") or []),
        updated_at=datetime.fromisoformat(updated) if updated else None,
        owner_user=data.get("owner_user"),
        owner_email=data.get("owner_email"),
    )


class AgentPlatformStateStore:
    """One control session (`m-<hash>`, user `silk-agent-control`) per conversation.

    Every `put` appends a `{"kind": "control", "seq", "state"}` event; the event with the
    largest seq is the current value. If the number of conversations makes `list_running`
    slow, swap this class for a Firestore implementation behind the same port.
    """

    def __init__(self, sessions: AgentPlatformSessions) -> None:
        self._api = sessions
        self._cache: dict[str, ConversationState] = {}

    async def put(self, state: ConversationState) -> None:
        session_id = control_session_id(state.conversation_key)
        await self._api.ensure_session(session_id, user_id=CONTROL_USER_ID)
        await self._api.append_event(
            session_id,
            {"kind": "control", "seq": self._api.next_seq(), "state": state_to_dict(state)},
        )
        self._cache[state.conversation_key] = state_from_dict(state_to_dict(state))

    async def get(self, conversation_key: str) -> ConversationState | None:
        cached = self._cache.get(conversation_key)
        if cached is not None:
            return state_from_dict(state_to_dict(cached))
        latest = await self._latest(control_session_id(conversation_key))
        if latest is not None:
            self._cache[conversation_key] = latest
            return state_from_dict(state_to_dict(latest))
        return None

    async def list_running(self) -> list[ConversationState]:
        running: list[ConversationState] = []
        for session_id in await self._api.list_sessions(CONTROL_USER_ID):
            state = await self._latest(session_id)
            if state is not None and state.status == "running":
                running.append(state)
        return running

    async def _latest(self, session_id: str) -> ConversationState | None:
        events = await self._api.list_raw_events(session_id)
        controls = [e for e in events or [] if e.get("kind") == "control"]
        if not controls:
            return None
        latest = max(controls, key=lambda e: e.get("seq", 0))
        return state_from_dict(latest["state"])
