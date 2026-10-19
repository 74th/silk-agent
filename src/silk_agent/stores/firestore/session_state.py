"""SessionStateStore backed by the Firestore conversation document.

Every write goes through the owner-fenced ``commit_turn_state`` transaction, so a Worker that
lost its assignment cannot overwrite the new owner's turn state.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.ports import ConversationState
from silk_agent.stores.firestore.conversations import FirestoreConversationStore

_ATTEMPTS = 6


class OwnershipLostError(RuntimeError):
    """The state write was fenced out; the caller must stop acting as the owner."""


class StateConflictError(RuntimeError):
    """The state write lost every race for the version, but the writer is still the owner."""


async def _read_state(
    conversations: FirestoreConversationStore, conversation_key: str
) -> ConversationState | None:
    record = await conversations.get_conversation(conversation_key)
    if record is None:
        return None
    state = ConversationState(
        conversation_key=conversation_key,
        agent_session_id=record.get("sdk_session_id"),
        status=record.get("turn_status", "idle"),
        turn_id=record.get("turn_id"),
        bot_instance_id=record.get("bot_instance_id"),
        progress_ref=dict(record.get("progress_ref") or {}),
        running_command_ids=list(record.get("running_command_ids") or []),
        owner_user=record.get("owner_user"),
        owner_email=record.get("owner_email"),
        progress_snapshot=dict(record.get("progress_snapshot") or {}),
        history_reset_reason=record.get("history_reset_reason"),
    )
    state.persisted = _owned_fields(state)
    return state


def _owned_fields(state: ConversationState) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "sdk_session_id": state.agent_session_id,
        "turn_status": state.status,
        "turn_id": state.turn_id,
        "bot_instance_id": state.bot_instance_id,
        "running_command_ids": list(state.running_command_ids),
    }
    if state.history_reset_reason:
        fields["history_reset_reason"] = state.history_reset_reason
    return fields


class FirestoreSessionStateStore:
    def __init__(
        self,
        conversations: FirestoreConversationStore,
        owner_for: Callable[[str], OwnershipToken | None],
    ) -> None:
        self._conversations = conversations
        self._owner_for = owner_for

    async def get(self, conversation_key: str) -> ConversationState | None:
        return await _read_state(self._conversations, conversation_key)

    async def put(self, state: ConversationState) -> None:
        owned = _owned_fields(state)
        # Only what changed since the state was read (everything, for one never read).
        patch = (
            owned
            if state.persisted is None
            else {k: v for k, v in owned.items() if state.persisted.get(k) != v}
        )
        if not patch:
            return
        owner = None
        for attempt in range(_ATTEMPTS):
            # Re-read each attempt: a lease renewal can replace the token between tries.
            owner = self._owner_for(state.conversation_key)
            if owner is None:
                raise OwnershipLostError("no active ownership for this conversation")
            record = await self._conversations.get_conversation(state.conversation_key)
            if record is None:
                raise OwnershipLostError("conversation does not exist")
            version = record.get("version", 1)
            if (
                await self._conversations.commit_turn_state(
                    owner=owner, expected_version=version, patch=patch
                )
                is not None
            ):
                state.persisted = owned
                return
            await asyncio.sleep(0.02 * (attempt + 1))  # others are writing: let them finish
        # The version kept moving, or this writer was fenced out. Only the second means the
        # caller must stop acting as the owner.
        if owner is not None and await self._conversations.holds_ownership(owner):
            raise StateConflictError("conversation state kept changing concurrently")
        raise OwnershipLostError("conversation state was fenced")

    async def list_running(self) -> list[ConversationState]:
        # Distributed recovery is driven by expired ownership, not a scan of running turns.
        return []


class FirestoreProgressStateStore:
    """Sender-side view: reads the conversation state and writes only progress fields."""

    def __init__(self, conversations: FirestoreConversationStore) -> None:
        self._conversations = conversations

    async def get(self, conversation_key: str) -> ConversationState | None:
        return await _read_state(self._conversations, conversation_key)

    async def put(self, state: ConversationState) -> None:
        await self._conversations.update_progress(
            state.conversation_key,
            progress_ref=state.progress_ref,
            progress_snapshot=state.progress_snapshot,
        )

    async def list_running(self) -> list[ConversationState]:
        return []
