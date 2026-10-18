"""Firestore partial updates for conversation management state."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional
from google.cloud.firestore_v1.base_query import FieldFilter

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.core.distributed_ports import ConversationStore
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import document_id
from silk_agent.stores.firestore.ownership import ownership_matches

_PATCH_FIELDS = frozenset(
    {
        "turn_id",
        "turn_status",
        "sdk_session_id",
        "stop_seq",
        "progress_ref",
        "progress_snapshot",
        "history_uncertain",
        "history_reset_reason",
        "running_command_ids",
        "bot_instance_id",
    }
)
_TURN_STATUSES = frozenset(
    {"idle", "running", "interrupted_crash", "interrupted_shutdown", "interrupted_user"}
)


class FirestoreConversationStore:
    def __init__(self, client: firestore.AsyncClient, *, collection_prefix: str = "silk-agent"):
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("collection_prefix must be non-empty and cannot contain '/'")
        self.client = client
        self.collection_prefix = collection_prefix

    def _conversation_id(self, conversation_key: str) -> str:
        return document_id(conversation_key, prefix="conversation")

    def _collection(self, name: str) -> firestore.AsyncCollectionReference:
        return self.client.collection(f"{self.collection_prefix}_{name}")

    async def get_conversation(self, conversation_key: str) -> Mapping[str, Any] | None:
        conversation_ref = self._collection("conversations").document(
            self._conversation_id(conversation_key)
        )
        try:
            snapshot = await conversation_ref.get()
            return snapshot.to_dict() if snapshot.exists else None
        except Exception as exc:
            raise convert_firestore_error("get_conversation", exc) from None

    async def list_waiting(self, *, limit: int = 100) -> tuple[str, ...]:
        """Return a bounded page of conversation keys awaiting Worker allocation."""
        if limit < 1:
            raise ValueError("limit must be positive")
        try:
            query = (
                self._collection("conversations")
                .where(filter=FieldFilter("allocation_state", "==", "waiting"))
                .limit(limit)
            )
            keys = []
            async for snapshot in query.stream():
                data = snapshot.to_dict() or {}
                key = data.get("conversation_key")
                if isinstance(key, str) and key:
                    keys.append(key)
            return tuple(keys)
        except Exception as exc:
            raise convert_firestore_error("list_waiting_conversations", exc) from None

    async def load_guard_history(self, conversation_key: str) -> tuple[str, ...]:
        conversation = await self.get_conversation(conversation_key)
        if conversation is None:
            return ()
        history = conversation.get("guard_history", [])
        if not isinstance(history, list):
            return ()
        return tuple(item for item in history if isinstance(item, str))[-5:]

    async def append_guard_history(
        self, *, owner: OwnershipToken, text: str, limit: int = 5
    ) -> bool:
        if not text or limit < 1:
            raise ValueError("text and positive history limit are required")
        conversation_id = self._conversation_id(owner.conversation_key)
        conversation_ref = self._collection("conversations").document(conversation_id)
        ownership_ref = self._collection("ownership").document(conversation_id)
        transaction = self.client.transaction()

        @async_transactional
        async def append(transaction: firestore.AsyncTransaction) -> bool:
            ownership_snapshot = await ownership_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            if not ownership_snapshot.exists or not conversation_snapshot.exists:
                return False
            conversation = conversation_snapshot.to_dict() or {}
            if not ownership_matches(
                ownership_snapshot.to_dict() or {},
                conversation,
                owner,
                now=datetime.now(UTC),
            ):
                return False
            current = conversation.get("guard_history", [])
            if not isinstance(current, list):
                current = []
            history = [item for item in current if isinstance(item, str)]
            history.append(text)
            transaction.update(
                conversation_ref,
                {"guard_history": history[-limit:], "version": conversation.get("version", 1) + 1},
            )
            return True

        try:
            return await append(transaction)
        except Exception as exc:
            raise convert_firestore_error("append_guard_history", exc) from None

    async def commit_turn_state(
        self,
        *,
        owner: OwnershipToken,
        expected_version: int,
        patch: Mapping[str, Any],
    ) -> int | None:
        if not patch or set(patch) - _PATCH_FIELDS:
            raise ValueError("patch contains no fields or unsupported conversation fields")
        _validate_patch(patch)
        conversation_id = self._conversation_id(owner.conversation_key)
        conversation_ref = self._collection("conversations").document(conversation_id)
        ownership_ref = self._collection("ownership").document(conversation_id)
        transaction = self.client.transaction()

        @async_transactional
        async def update(transaction: firestore.AsyncTransaction) -> int | None:
            ownership_snapshot = await ownership_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            if not ownership_snapshot.exists or not conversation_snapshot.exists:
                return None
            conversation = conversation_snapshot.to_dict() or {}
            if (
                not ownership_matches(
                    ownership_snapshot.to_dict() or {},
                    conversation,
                    owner,
                    now=datetime.now(UTC),
                )
                or conversation.get("version", 1) != expected_version
            ):
                return None
            current_stop_seq = conversation.get("stop_seq", 0)
            if "stop_seq" in patch and patch["stop_seq"] < current_stop_seq:
                return None
            if conversation.get("reset_pending") and patch.get("turn_status") == "running":
                return None
            new_version = expected_version + 1
            transaction.update(conversation_ref, {**patch, "version": new_version})
            return new_version

        try:
            return await update(transaction)
        except Exception as exc:
            raise convert_firestore_error("commit_turn_state", exc) from None

    async def allocate_output_sequence(self, conversation_key: str) -> int:
        """Reserve the next per-conversation Outbox sequence; never reset with generation."""
        conversation_ref = self._collection("conversations").document(
            self._conversation_id(conversation_key)
        )
        transaction = self.client.transaction()

        @async_transactional
        async def allocate(transaction: firestore.AsyncTransaction) -> int:
            snapshot = await conversation_ref.get(transaction=transaction)
            if not snapshot.exists:
                raise ValueError("conversation does not exist")
            current = (snapshot.to_dict() or {}).get("next_output_seq", 1)
            if not isinstance(current, int) or isinstance(current, bool) or current < 1:
                raise ValueError("conversation next_output_seq is invalid")
            transaction.update(conversation_ref, {"next_output_seq": current + 1})
            return current

        try:
            return await allocate(transaction)
        except ValueError:
            raise
        except Exception as exc:
            raise convert_firestore_error("allocate_output_sequence", exc) from None

    async def update_progress(
        self,
        conversation_key: str,
        *,
        progress_ref: Mapping[str, Any],
        progress_snapshot: Mapping[str, Any],
    ) -> None:
        """Sender-owned progress fields; they do not bump the owner's state version."""
        patch = {"progress_ref": dict(progress_ref), "progress_snapshot": dict(progress_snapshot)}
        _validate_patch(patch)
        conversation_ref = self._collection("conversations").document(
            self._conversation_id(conversation_key)
        )
        try:
            await conversation_ref.update(patch)
        except Exception as exc:
            raise convert_firestore_error("update_progress", exc) from None

    async def mark_recovery_notified(self, conversation_key: str, epoch: int) -> bool:
        """True exactly once per ownership epoch (the user is told a recovery is pending)."""
        conversation_ref = self._collection("conversations").document(
            self._conversation_id(conversation_key)
        )
        transaction = self.client.transaction()

        @async_transactional
        async def mark(transaction: firestore.AsyncTransaction) -> bool:
            snapshot = await conversation_ref.get(transaction=transaction)
            if not snapshot.exists:
                return False
            if (snapshot.to_dict() or {}).get("recovery_notice_epoch") == epoch:
                return False
            transaction.update(conversation_ref, {"recovery_notice_epoch": epoch})
            return True

        try:
            return await mark(transaction)
        except Exception as exc:
            raise convert_firestore_error("mark_recovery_notified", exc) from None

    async def begin_reset(self, *, owner: OwnershipToken, reset_seq: int) -> bool:
        if reset_seq < 1:
            raise ValueError("reset_seq must be positive")
        conversation_id = self._conversation_id(owner.conversation_key)
        conversation_ref = self._collection("conversations").document(conversation_id)
        ownership_ref = self._collection("ownership").document(conversation_id)
        transaction = self.client.transaction()

        @async_transactional
        async def begin(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await ownership_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            if not owner_snapshot.exists or not conversation_snapshot.exists:
                return False
            conversation = conversation_snapshot.to_dict() or {}
            if not ownership_matches(
                owner_snapshot.to_dict() or {}, conversation, owner, now=datetime.now(UTC)
            ):
                return False
            if conversation.get("reset_pending"):
                return conversation.get("reset_seq") == reset_seq
            transaction.update(
                conversation_ref,
                {
                    "reset_pending": True,
                    "reset_seq": reset_seq,
                    "stop_seq": max(conversation.get("stop_seq", 0), reset_seq),
                    "version": conversation.get("version", 1) + 1,
                },
            )
            return True

        try:
            return await begin(transaction)
        except Exception as exc:
            raise convert_firestore_error("begin_reset", exc) from None

    async def complete_reset(
        self, *, owner: OwnershipToken, now: datetime
    ) -> OwnershipToken | None:
        conversation_id = self._conversation_id(owner.conversation_key)
        conversation_ref = self._collection("conversations").document(conversation_id)
        ownership_ref = self._collection("ownership").document(conversation_id)
        transaction = self.client.transaction()

        @async_transactional
        async def complete(transaction: firestore.AsyncTransaction) -> OwnershipToken | None:
            owner_snapshot = await ownership_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            if not owner_snapshot.exists or not conversation_snapshot.exists:
                return None
            owner_data = owner_snapshot.to_dict() or {}
            conversation = conversation_snapshot.to_dict() or {}
            if (
                not ownership_matches(owner_data, conversation, owner, now=now)
                or not conversation.get("reset_pending")
                or conversation.get("turn_status", "idle") == "running"
            ):
                return None
            new_token = OwnershipToken(
                owner.conversation_key,
                owner.worker_id,
                owner.worker_incarnation,
                owner.epoch + 1,
                owner.conversation_generation + 1,
                owner.expires_at,
            )
            transaction.update(
                conversation_ref,
                {
                    "conversation_generation": new_token.conversation_generation,
                    "sdk_session_id": None,
                    "turn_id": None,
                    "turn_status": "idle",
                    "guard_history": [],
                    "history_uncertain": False,
                    "history_reset_reason": firestore.DELETE_FIELD,
                    "stop_seq": 0,
                    "reset_pending": False,
                    "reset_seq": None,
                    "version": conversation.get("version", 1) + 1,
                },
            )
            transaction.update(ownership_ref, new_token.to_dict())
            return new_token

        try:
            return await complete(transaction)
        except Exception as exc:
            raise convert_firestore_error("complete_reset", exc) from None

    async def set_initial_owner_once(
        self,
        *,
        owner: OwnershipToken,
        owner_user: str,
        owner_email: str | None,
    ) -> bool:
        if not owner_user:
            raise ValueError("owner_user must be a non-empty string")
        if owner_email is not None and not isinstance(owner_email, str):
            raise ValueError("owner_email must be a string or None")
        conversation_id = self._conversation_id(owner.conversation_key)
        conversation_ref = self._collection("conversations").document(conversation_id)
        ownership_ref = self._collection("ownership").document(conversation_id)
        transaction = self.client.transaction()

        @async_transactional
        async def set_owner(transaction: firestore.AsyncTransaction) -> bool:
            ownership_snapshot = await ownership_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            if not ownership_snapshot.exists or not conversation_snapshot.exists:
                return False
            conversation = conversation_snapshot.to_dict() or {}
            if not ownership_matches(
                ownership_snapshot.to_dict() or {},
                conversation,
                owner,
                now=datetime.now(UTC),
            ) or conversation.get("owner_user"):
                return False
            transaction.update(
                conversation_ref,
                {
                    "owner_user": owner_user,
                    "owner_email": owner_email,
                    "version": conversation.get("version", 1) + 1,
                },
            )
            return True

        try:
            return await set_owner(transaction)
        except Exception as exc:
            raise convert_firestore_error("set_initial_owner", exc) from None


def _validate_patch(patch: Mapping[str, Any]) -> None:
    if (
        "turn_id" in patch
        and patch["turn_id"] is not None
        and not isinstance(patch["turn_id"], str)
    ):
        raise ValueError("turn_id must be a string or None")
    if "turn_status" in patch and patch["turn_status"] not in _TURN_STATUSES:
        raise ValueError("turn_status is unknown")
    if (
        "sdk_session_id" in patch
        and patch["sdk_session_id"] is not None
        and not isinstance(patch["sdk_session_id"], str)
    ):
        raise ValueError("sdk_session_id must be a string or None")
    if "stop_seq" in patch:
        stop_seq = patch["stop_seq"]
        if not isinstance(stop_seq, int) or isinstance(stop_seq, bool) or stop_seq < 0:
            raise ValueError("stop_seq must be a non-negative integer")
    if "progress_ref" in patch and not isinstance(patch["progress_ref"], Mapping):
        raise ValueError("progress_ref must be a mapping")
    if "progress_snapshot" in patch and not isinstance(patch["progress_snapshot"], Mapping):
        raise ValueError("progress_snapshot must be a mapping")
    if "history_uncertain" in patch and not isinstance(patch["history_uncertain"], bool):
        raise ValueError("history_uncertain must be a bool")
    if "history_reset_reason" in patch and not isinstance(patch["history_reset_reason"], str):
        raise ValueError("history_reset_reason must be a string")
    if "running_command_ids" in patch and (
        not isinstance(patch["running_command_ids"], list)
        or not all(isinstance(item, str) for item in patch["running_command_ids"])
    ):
        raise ValueError("running_command_ids must be a list of strings")
    if (
        "bot_instance_id" in patch
        and patch["bot_instance_id"] is not None
        and not isinstance(patch["bot_instance_id"], str)
    ):
        raise ValueError("bot_instance_id must be a string or None")


def conversation_store(
    client: firestore.AsyncClient, *, collection_prefix: str = "silk-agent"
) -> ConversationStore:
    return FirestoreConversationStore(client, collection_prefix=collection_prefix)
