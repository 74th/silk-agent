"""SessionStateStore backed by the Firestore conversation document.

Every write goes through the owner-fenced ``commit_turn_state`` transaction, so a Worker that
lost its assignment cannot overwrite the new owner's turn state.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.ports import ConversationState
from silk_agent.stores.firestore.conversations import FirestoreConversationStore

_ATTEMPTS = 3


class OwnershipLostError(RuntimeError):
    """The state write was fenced out; the caller must stop acting as the owner."""


async def _read_state(
    conversations: FirestoreConversationStore, conversation_key: str
) -> ConversationState | None:
    record = await conversations.get_conversation(conversation_key)
    if record is None:
        return None
    return ConversationState(
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
    )


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
        patch: dict[str, Any] = {
            "sdk_session_id": state.agent_session_id,
            "turn_status": state.status,
            "turn_id": state.turn_id,
            "bot_instance_id": state.bot_instance_id,
            "running_command_ids": list(state.running_command_ids),
        }
        for _ in range(_ATTEMPTS):
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
                return
        raise OwnershipLostError("conversation state was fenced or changed concurrently")

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
