"""Firestore leases for distributed command status monitors."""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional
from google.cloud.firestore_v1.base_query import FieldFilter

from silk_agent.core.distributed_models import (
    CommandExecutionState,
    InputEnvelope,
    NotificationState,
    validate_state_transition,
)
from silk_agent.stores.firestore.codec import encode_record
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import conversation_document_id, document_id
from silk_agent.stores.firestore.retention import RetentionPolicy, command_expiry


class FirestoreCommandMonitorStore:
    """Claim nonterminal command records without making tracker start requests."""

    def __init__(
        self,
        client: firestore.AsyncClient,
        *,
        collection_prefix: str = "silk-agent",
        retention: RetentionPolicy | None = None,
    ):
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("collection_prefix must be non-empty and cannot contain '/'")
        self.client = client
        self.collection_prefix = collection_prefix
        self.retention = retention or RetentionPolicy()

    def _collection(self) -> firestore.AsyncCollectionReference:
        return self.client.collection(f"{self.collection_prefix}_commands")

    def _named_collection(self, name: str) -> firestore.AsyncCollectionReference:
        return self.client.collection(f"{self.collection_prefix}_{name}")

    def _ref(self, command_id: str) -> firestore.AsyncDocumentReference:
        return self._collection().document(document_id(command_id, prefix="command"))

    async def get_command(self, command_id: str) -> Mapping[str, Any] | None:
        """Read the durable command and notification state by stable command ID."""
        try:
            snapshot = await self._ref(command_id).get()
            if not snapshot.exists:
                return None
            return {**(snapshot.to_dict() or {}), "command_id": command_id}
        except Exception as exc:
            raise convert_firestore_error("get_command", exc) from None

    async def set_notification_state(
        self, command_id: str, *, expected: str, new_state: str, now: datetime
    ) -> bool:
        _validate_time(now, "now")
        allowed = {"pending", "held", "delivered", "suppressed"}
        if expected not in allowed or new_state not in allowed:
            raise ValueError("notification state is unknown")
        ref = self._ref(command_id)
        transaction = self.client.transaction()

        @async_transactional
        async def update(transaction: firestore.AsyncTransaction) -> bool:
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return False
            data = snapshot.to_dict() or {}
            if data.get("notification_state") != expected:
                return False
            patch = {"notification_state": new_state, "notification_updated_at": now}
            patch.update(
                command_expiry_patch(
                    data,
                    state=data.get("state"),
                    notification_state=new_state,
                    terminal_at=data.get("completed_at"),
                    notified_at=now,
                    now=now,
                    retention=self.retention,
                )
            )
            operation_ref = await operation_to_stamp(
                self.client, self.collection_prefix, transaction, command_id, patch
            )
            transaction.update(ref, patch)
            stamp_operation_expiry(transaction, operation_ref, patch)
            return True

        try:
            return await update(transaction)
        except Exception as exc:
            raise convert_firestore_error("set_notification_state", exc) from None

    async def list_due(self, *, now: datetime, limit: int = 100) -> Sequence[Mapping[str, Any]]:
        _validate_time(now, "now")
        if limit < 1:
            raise ValueError("limit must be positive")
        query = (
            self._collection()
            .where(
                filter=FieldFilter(
                    "state",
                    "in",
                    [
                        CommandExecutionState.INTENT.value,
                        CommandExecutionState.RUNNING.value,
                        CommandExecutionState.CANCEL_REQUESTED.value,
                    ],
                )
            )
            .where(filter=FieldFilter("next_attempt_at", "<=", now))
            .order_by("next_attempt_at")
            .limit(limit)
        )
        try:
            return [snapshot.to_dict() or {} async for snapshot in query.stream()]
        except Exception as exc:
            raise convert_firestore_error("list_due_commands", exc) from None

    async def list_for_stop(
        self,
        *,
        conversation_key: str,
        conversation_generation: int,
        stop_seq: int,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[Sequence[Mapping[str, Any]], str | None]:
        if not conversation_key or conversation_generation < 1 or stop_seq < 0 or limit < 1:
            raise ValueError("stop command query parameters are invalid")
        query = (
            self._collection()
            .where(filter=FieldFilter("conversation_key", "==", conversation_key))
            .where(filter=FieldFilter("conversation_generation", "==", conversation_generation))
            .where(filter=FieldFilter("state", "in", ["intent", "running", "cancel_requested"]))
            .where(filter=FieldFilter("origin_seq", "<=", stop_seq))
            .order_by("origin_seq")
            .limit(limit + 1)
        )
        try:
            if cursor is not None:
                cursor_snapshot = await self._collection().document(cursor).get()
                if not cursor_snapshot.exists:
                    raise ValueError("command stop cursor does not exist")
                query = query.start_after(cursor_snapshot)
            snapshots = []
            async for snapshot in query.stream():
                snapshots.append(snapshot)
            next_cursor = None
            if len(snapshots) > limit:
                next_cursor = snapshots[limit - 1].id
            records = [snapshot.to_dict() or {} for snapshot in snapshots[:limit]]
            return tuple(records), next_cursor
        except Exception as exc:
            raise convert_firestore_error("list_stop_commands", exc) from None

    async def claim(
        self,
        command_id: str,
        *,
        monitor_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> Mapping[str, Any] | None:
        _validate_time(now, "now")
        _validate_time(lease_expires_at, "lease_expires_at")
        if not monitor_id or lease_expires_at <= now:
            raise ValueError("monitor_id and a future lease_expires_at are required")
        ref = self._ref(command_id)
        token = uuid.uuid4().hex
        transaction = self.client.transaction()

        @async_transactional
        async def claim_in_transaction(transaction: firestore.AsyncTransaction):
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return None
            data = snapshot.to_dict() or {}
            if data.get("state") not in {
                CommandExecutionState.INTENT.value,
                CommandExecutionState.RUNNING.value,
                CommandExecutionState.CANCEL_REQUESTED.value,
            }:
                return None
            due = data.get("next_attempt_at")
            if not isinstance(due, datetime) or due > now:
                return None
            current_expiry = data.get("monitor_lease_expires_at")
            if isinstance(current_expiry, datetime) and current_expiry > now:
                return None
            transaction.update(
                ref,
                {
                    "monitor_lease_owner": monitor_id,
                    "monitor_lease_token": token,
                    "monitor_lease_expires_at": lease_expires_at,
                },
            )
            return {**data, "command_id": command_id, "monitor_lease_token": token}

        try:
            return await claim_in_transaction(transaction)
        except Exception as exc:
            raise convert_firestore_error("claim_command", exc) from None

    async def set_unreadable_since(
        self, command_id: str, *, claim_token: str, since: datetime | None
    ) -> bool:
        if since is not None:
            _validate_time(since, "since")
        ref = self._ref(command_id)
        transaction = self.client.transaction()

        @async_transactional
        async def update(transaction: firestore.AsyncTransaction) -> bool:
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return False
            data = snapshot.to_dict() or {}
            if data.get("monitor_lease_token") != claim_token:
                return False
            transaction.update(
                ref,
                {"unreadable_since": since if since is not None else firestore.DELETE_FIELD},
            )
            return True

        try:
            return await update(transaction)
        except Exception as exc:
            raise convert_firestore_error("set_command_unreadable_since", exc) from None

    async def record_observation(
        self,
        command_id: str,
        *,
        claim_token: str,
        expected: CommandExecutionState,
        observed: CommandExecutionState | None,
        now: datetime,
        retry_at: datetime,
    ) -> bool:
        _validate_time(now, "now")
        _validate_time(retry_at, "retry_at")
        ref = self._ref(command_id)
        transaction = self.client.transaction()

        @async_transactional
        async def record_in_transaction(transaction: firestore.AsyncTransaction) -> bool:
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return False
            data = snapshot.to_dict() or {}
            if data.get("monitor_lease_token") != claim_token:
                return False
            if data.get("state") != expected.value:
                return False
            if observed is None or observed is expected:
                # Keep the current retry timestamp. The lease expiry naturally enables
                # the next poll, so an unchanged observation causes no Firestore write.
                return True
            transaction.update(
                ref,
                {
                    "state": observed.value,
                    "updated_at": now,
                    "next_attempt_at": retry_at,
                    "monitor_lease_owner": firestore.DELETE_FIELD,
                    "monitor_lease_token": firestore.DELETE_FIELD,
                    "monitor_lease_expires_at": firestore.DELETE_FIELD,
                },
            )
            return True

        try:
            return await record_in_transaction(transaction)
        except Exception as exc:
            raise convert_firestore_error("record_command_observation", exc) from None

    async def detach(self, command_id: str, *, now: datetime) -> Mapping[str, Any] | None:
        """Race a foreground waiter against terminal observation in one transaction."""
        _validate_time(now, "now")
        ref = self._ref(command_id)
        transaction = self.client.transaction()

        @async_transactional
        async def detach_in_transaction(transaction: firestore.AsyncTransaction):
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return None
            data = snapshot.to_dict() or {}
            mode = data.get("delivery_mode", "foreground")
            state = data.get("state")
            if state in _TERMINAL_STATES:
                return {"decision": "return_result", **data}
            if mode == "foreground":
                transaction.update(
                    ref,
                    {"delivery_mode": "background", "detached_at": now, "updated_at": now},
                )
                return {"decision": "background", **data, "delivery_mode": "background"}
            return {"decision": "background", **data}

        try:
            return await detach_in_transaction(transaction)
        except Exception as exc:
            raise convert_firestore_error("detach_command", exc) from None

    async def orphan(self, command_id: str, *, now: datetime) -> Mapping[str, Any] | None:
        _validate_time(now, "now")
        ref = self._ref(command_id)
        transaction = self.client.transaction()

        @async_transactional
        async def orphan_in_transaction(transaction: firestore.AsyncTransaction):
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return None
            data = snapshot.to_dict() or {}
            if data.get("state") in _TERMINAL_STATES:
                return {**data, "decision": "return_result"}
            if data.get("delivery_mode", "foreground") == "foreground":
                transaction.update(
                    ref,
                    {"delivery_mode": "orphaned", "orphaned_at": now, "updated_at": now},
                )
                data.update(delivery_mode="orphaned", orphaned_at=now)
            return {**data, "decision": data.get("delivery_mode", "foreground")}

        try:
            return await orphan_in_transaction(transaction)
        except Exception as exc:
            raise convert_firestore_error("orphan_command", exc) from None

    async def request_cancel(
        self, command_id: str, *, reason: str, now: datetime
    ) -> Mapping[str, Any] | None:
        if not reason:
            raise ValueError("reason must not be empty")
        _validate_time(now, "now")
        ref = self._ref(command_id)
        transaction = self.client.transaction()
        request_id = uuid.uuid4().hex

        @async_transactional
        async def request_in_transaction(transaction: firestore.AsyncTransaction):
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return None
            data = snapshot.to_dict() or {}
            state = CommandExecutionState(data.get("state"))
            stored_request_id = data.get("cancel_request_id") or request_id
            update: dict[str, Any] = {
                "cancel_request_id": stored_request_id,
                "cancel_reason": reason,
                "cancel_requested_at": data.get("cancel_requested_at", now),
                "notification_suppressed": True,
                "updated_at": now,
            }
            if state in _TERMINAL_STATES:
                update["cancel_outcome"] = "too_late"
            else:
                if state is not CommandExecutionState.CANCEL_REQUESTED:
                    validate_state_transition(state, CommandExecutionState.CANCEL_REQUESTED)
                    update["state"] = CommandExecutionState.CANCEL_REQUESTED.value
                update["cancel_outcome"] = data.get("cancel_outcome", "pending")
            if data.get("notification_state") == "pending":
                update["notification_state"] = "suppressed"
                update.update(
                    command_expiry_patch(
                        data,
                        state=data.get("state"),
                        notification_state="suppressed",
                        terminal_at=data.get("completed_at"),
                        notified_at=now,
                        now=now,
                        retention=self.retention,
                    )
                )
            operation_ref = await operation_to_stamp(
                self.client, self.collection_prefix, transaction, command_id, update
            )
            transaction.update(ref, update)
            stamp_operation_expiry(transaction, operation_ref, update)
            return {**data, **update, "command_id": command_id}

        try:
            return await request_in_transaction(transaction)
        except Exception as exc:
            raise convert_firestore_error("request_command_cancel", exc) from None

    async def record_cancel_result(
        self,
        command_id: str,
        *,
        request_id: str,
        outcome: str,
        now: datetime,
    ) -> bool:
        if outcome not in {"accepted", "unknown", "rejected"}:
            raise ValueError("outcome must be accepted, unknown, or rejected")
        _validate_time(now, "now")
        ref = self._ref(command_id)
        transaction = self.client.transaction()

        @async_transactional
        async def record_in_transaction(transaction: firestore.AsyncTransaction) -> bool:
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return False
            data = snapshot.to_dict() or {}
            if (
                data.get("cancel_request_id") != request_id
                or data.get("state") != CommandExecutionState.CANCEL_REQUESTED.value
            ):
                return False
            transaction.update(
                ref,
                {"cancel_outcome": outcome, "cancel_response_at": now, "updated_at": now},
            )
            return True

        try:
            return await record_in_transaction(transaction)
        except Exception as exc:
            raise convert_firestore_error("record_command_cancel", exc) from None

    async def observe_terminal(
        self,
        command_id: str,
        *,
        claim_token: str,
        expected: CommandExecutionState,
        terminal: CommandExecutionState,
        result: Mapping[str, Any],
        now: datetime,
    ) -> Mapping[str, Any] | None:
        """Persist terminal observation with exactly one foreground/notice decision."""
        _validate_time(now, "now")
        try:
            result_size = len(
                json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("command result metadata must be JSON serializable") from exc
        if result_size > 64 * 1024:
            raise ValueError("command result metadata exceeds the Firestore size budget")
        if terminal not in {
            CommandExecutionState.FINISHED,
            CommandExecutionState.CANCELLED,
            CommandExecutionState.UNKNOWN,
        }:
            raise ValueError("terminal must be a terminal command state")
        ref = self._ref(command_id)
        transaction = self.client.transaction()

        @async_transactional
        async def finish_in_transaction(transaction: firestore.AsyncTransaction):
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return None
            data = snapshot.to_dict() or {}
            if data.get("monitor_lease_token") != claim_token:
                return None
            if data.get("state") != expected.value:
                return None
            validate_state_transition(expected, terminal)
            mode = data.get("delivery_mode", "foreground")
            suppressed = data.get("notification_suppressed") is True
            decision = (
                "suppressed"
                if suppressed
                else "return_result"
                if mode == "foreground"
                else "notify"
            )
            notification_state = data.get("notification_state", "none")
            if decision == "notify" and notification_state == "none":
                notification_state = "pending"
            elif decision == "suppressed":
                notification_state = "suppressed"
            expiry_patch = command_expiry_patch(
                data,
                state=terminal.value,
                notification_state=notification_state,
                terminal_at=now,
                notified_at=now if notification_state == "suppressed" else None,
                now=now,
                retention=self.retention,
            )
            operation_ref = await operation_to_stamp(
                self.client, self.collection_prefix, transaction, command_id, expiry_patch
            )
            completion_input = None
            if decision == "notify":
                conversation_key = data.get("conversation_key")
                generation = data.get("conversation_generation")
                sandbox_generation = data.get("sandbox_generation")
                if (
                    not isinstance(conversation_key, str)
                    or not isinstance(generation, int)
                    or isinstance(generation, bool)
                    or not isinstance(sandbox_generation, int)
                    or isinstance(sandbox_generation, bool)
                ):
                    return None
                completion_id = f"command-finished:{sandbox_generation}:{command_id}"
                conversation_ref = self._named_collection("conversations").document(
                    conversation_document_id(conversation_key)
                )
                input_ref = conversation_ref.collection("inputs").document(
                    document_id(completion_id, prefix="input")
                )
                conversation_snapshot = await conversation_ref.get(transaction=transaction)
                input_snapshot = await input_ref.get(transaction=transaction)
                if input_snapshot.exists:
                    completion_input = input_snapshot.to_dict() or {}
                else:
                    conversation_data = conversation_snapshot.to_dict() or {}
                    stored_generation = conversation_data.get("conversation_generation")
                    if stored_generation is not None and stored_generation != generation:
                        return None
                    seq = conversation_data.get("next_input_seq", 1)
                    version = conversation_data.get("version", 0)
                    if (
                        not isinstance(seq, int)
                        or isinstance(seq, bool)
                        or seq < 1
                        or not isinstance(version, int)
                        or isinstance(version, bool)
                        or version < 0
                    ):
                        return None
                    completion_input = InputEnvelope(
                        conversation_key,
                        seq,
                        completion_id,
                        generation,
                        "command_finished",
                        {
                            "command_id": command_id,
                            "sandbox_generation": sandbox_generation,
                            "state": terminal.value,
                            "result": dict(result),
                        },
                        now,
                    )
                    transaction.set(
                        conversation_ref,
                        {
                            "conversation_key": conversation_key,
                            "conversation_generation": generation,
                            "next_input_seq": seq + 1,
                            "allocation_state": "waiting",
                            "version": version + 1,
                            "schema_version": 1,
                        },
                        merge=True,
                    )
                    transaction.create(input_ref, encode_record(completion_input))
            stamp_operation_expiry(transaction, operation_ref, expiry_patch)
            transaction.update(
                ref,
                {
                    "state": terminal.value,
                    "result": dict(result),
                    "updated_at": now,
                    "completed_at": now,
                    "notification_state": notification_state,
                    "terminal_delivery": decision,
                    **expiry_patch,
                    **(
                        {"cancel_confirmed_at": now}
                        if terminal is CommandExecutionState.CANCELLED
                        else {}
                    ),
                    **(
                        {"cancel_outcome": "confirmed"}
                        if terminal is CommandExecutionState.CANCELLED
                        else {"cancel_outcome": "not_confirmed"}
                        if expected is CommandExecutionState.CANCEL_REQUESTED
                        else {}
                    ),
                    "monitor_lease_owner": firestore.DELETE_FIELD,
                    "monitor_lease_token": firestore.DELETE_FIELD,
                    "monitor_lease_expires_at": firestore.DELETE_FIELD,
                },
            )
            return {
                **data,
                "state": terminal.value,
                "result": dict(result),
                "decision": decision,
                "completion_input": completion_input,
            }

        try:
            return await finish_in_transaction(transaction)
        except Exception as exc:
            raise convert_firestore_error("finish_command", exc) from None


def command_expiry_patch(
    data: Mapping[str, Any],
    *,
    state: Any,
    notification_state: Any,
    terminal_at: Any,
    notified_at: datetime | None,
    now: datetime,
    retention: RetentionPolicy,
) -> dict[str, Any]:
    """`expire_at` for a command after a state change: set, removed, or left alone.

    The expiry counts from the later of the command's end and its notice. A command whose
    notice is pending or held keeps (or loses) its `expire_at` so it is never deleted early.
    """
    try:
        command_state = CommandExecutionState(state)
        notice = NotificationState(notification_state)
    except ValueError:
        return {}
    expiry = command_expiry(
        command_state,
        notice,
        terminal_at if isinstance(terminal_at, datetime) else now,
        notified_at,
        retention=retention.terminal,
    )
    if expiry is not None:
        return {"expire_at": expiry}
    if data.get("expire_at") is not None:
        return {"expire_at": firestore.DELETE_FIELD}
    return {}


async def operation_to_stamp(
    client: firestore.AsyncClient,
    collection_prefix: str,
    transaction: firestore.AsyncTransaction,
    command_id: str,
    patch: Mapping[str, Any],
) -> firestore.AsyncDocumentReference | None:
    """The Sandbox operation (same ID as the command) that must follow a command's `expire_at`.

    This reads, so call it before the transaction's first write.
    """
    if "expire_at" not in patch:
        return None
    operation_ref = client.collection(f"{collection_prefix}_sandbox_operations").document(
        document_id(command_id, prefix="operation")
    )
    return operation_ref if (await operation_ref.get(transaction=transaction)).exists else None


def stamp_operation_expiry(
    transaction: firestore.AsyncTransaction,
    operation_ref: firestore.AsyncDocumentReference | None,
    patch: Mapping[str, Any],
) -> None:
    if operation_ref is not None:
        transaction.update(operation_ref, {"expire_at": patch["expire_at"]})


def _validate_time(value: datetime, field_name: str) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be UTC-aware")


_TERMINAL_STATES = frozenset(
    {
        CommandExecutionState.FINISHED.value,
        CommandExecutionState.CANCELLED.value,
        CommandExecutionState.UNKNOWN.value,
    }
)
