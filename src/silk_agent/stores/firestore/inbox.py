"""Firestore transaction adapter for durable input acceptance."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional
from google.cloud.firestore_v1.base_query import FieldFilter

from silk_agent.core.distributed_models import (
    ControlEnvelope,
    InputDeliveryState,
    InputEnvelope,
    OwnershipToken,
    validate_state_transition,
)
from silk_agent.core.distributed_ports import InboxStore
from silk_agent.stores.firestore.codec import decode_record, encode_record
from silk_agent.stores.firestore.errors import FirestoreStoreError, convert_firestore_error
from silk_agent.stores.firestore.ids import document_id
from silk_agent.stores.firestore.ownership import ownership_matches


class FirestoreInboxStore:
    def __init__(self, client: firestore.AsyncClient, *, collection_prefix: str = "silk-agent"):
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("collection_prefix must be non-empty and cannot contain '/'")
        self.client = client
        self.collection_prefix = collection_prefix

    def _collection(self, name: str) -> firestore.AsyncCollectionReference:
        return self.client.collection(f"{self.collection_prefix}_{name}")

    def _conversation_id(self, conversation_key: str) -> str:
        return document_id(conversation_key, prefix="conversation")

    async def accept_input(
        self,
        *,
        conversation_key: str,
        input_id: str,
        conversation_generation: int,
        kind: str,
        payload: Mapping[str, Any],
        created_at: datetime,
    ) -> InputEnvelope:
        result, _ = await self._accept(
            conversation_key=conversation_key,
            input_id=input_id,
            conversation_generation=conversation_generation,
            kind=kind,
            payload=payload,
            created_at=created_at,
            sender_id=None,
            receipt_refs=(),
        )
        return cast(InputEnvelope, result)

    async def accept_control(
        self,
        *,
        conversation_key: str,
        input_id: str,
        conversation_generation: int,
        kind: str,
        sender_id: str,
        payload: Mapping[str, Any],
        created_at: datetime,
    ) -> ControlEnvelope:
        result, _ = await self._accept(
            conversation_key=conversation_key,
            input_id=input_id,
            conversation_generation=conversation_generation,
            kind=kind,
            payload=payload,
            created_at=created_at,
            sender_id=sender_id,
            receipt_refs=(),
        )
        return cast(ControlEnvelope, result)

    async def accept_received(
        self,
        *,
        conversation_key: str,
        input_id: str,
        conversation_generation: int,
        kind: str,
        payload: Mapping[str, Any],
        created_at: datetime,
        workspace: str,
        team: str,
        channel: str,
        message_ts: str,
        event_id: str,
        sender_id: str | None = None,
        participating: bool = False,
    ) -> InputEnvelope | ControlEnvelope:
        """Atomically deduplicate event and message delivery before returning an envelope."""
        event_receipt_id = document_id("event", workspace, event_id, prefix="receipt")
        message_receipt_id = document_id(
            "message", workspace, team, channel, message_ts, prefix="receipt"
        )
        arguments = {
            "conversation_key": conversation_key,
            "input_id": input_id,
            "conversation_generation": conversation_generation,
            "kind": kind,
            "payload": payload,
            "created_at": created_at,
            "sender_id": sender_id,
            "participating": participating,
            "receipt_refs": (
                (self._collection("slack_receipts").document(event_receipt_id), "event"),
                (self._collection("slack_receipts").document(message_receipt_id), "message"),
            ),
        }
        try:
            result, _ = await self._accept(**arguments)
        except FirestoreStoreError as exc:
            # The emulator can exhaust the transaction SDK's retry budget during a
            # duplicate event race. Repeating this same idempotent receipt transaction
            # resolves an unknown commit by reading the durable receipt before writing.
            if exc.status is not None:
                raise
            result, _ = await self._accept(**arguments)
        return result

    async def _accept(
        self,
        *,
        conversation_key: str,
        input_id: str,
        conversation_generation: int,
        kind: str,
        payload: Mapping[str, Any],
        created_at: datetime,
        sender_id: str | None,
        participating: bool = False,
        receipt_refs: tuple[tuple[firestore.AsyncDocumentReference, str], ...],
    ) -> tuple[InputEnvelope | ControlEnvelope, bool]:
        conversation_id = self._conversation_id(conversation_key)
        conversation_ref = self._collection("conversations").document(conversation_id)
        collection_name = "controls" if sender_id is not None else "inputs"
        record_id = document_id(input_id, prefix="input")
        record_ref = conversation_ref.collection(collection_name).document(record_id)
        transaction = self.client.transaction()

        @async_transactional
        async def write(transaction: firestore.AsyncTransaction) -> tuple[dict[str, Any], bool]:
            record_snapshot = await record_ref.get(transaction=transaction)
            receipt_snapshots = []
            for receipt_ref, _ in receipt_refs:
                receipt_snapshots.append(await receipt_ref.get(transaction=transaction))
            prior_input_ids = {
                snapshot.get("input_id")
                for snapshot in receipt_snapshots
                if snapshot.exists and snapshot.get("input_id")
            }
            if record_snapshot.exists:
                record_data = cast(dict[str, Any], record_snapshot.to_dict())
                expected_data = {
                    "conversation_key": conversation_key,
                    "conversation_generation": conversation_generation,
                    "kind": kind,
                    "payload": dict(payload),
                }
                if sender_id is not None:
                    expected_data["sender_id"] = sender_id
                if any(record_data.get(key) != value for key, value in expected_data.items()):
                    raise FirestoreStoreError("input_conflict")
                if prior_input_ids and prior_input_ids != {input_id}:
                    raise FirestoreStoreError("deduplicate")
                for (receipt_ref, receipt_kind), snapshot in zip(
                    receipt_refs, receipt_snapshots, strict=True
                ):
                    if not snapshot.exists:
                        transaction.create(
                            receipt_ref,
                            {
                                "input_id": input_id,
                                "record_collection": collection_name,
                                "conversation_id": conversation_id,
                                "receipt_kind": receipt_kind,
                                "schema_version": 1,
                            },
                        )
                if participating:
                    transaction.set(conversation_ref, {"participating": True}, merge=True)
                return record_data, False

            if prior_input_ids:
                if len(prior_input_ids) != 1:
                    raise FirestoreStoreError("deduplicate")
                prior_id = next(iter(prior_input_ids))
                receipt_data = next(
                    snapshot.to_dict() or {}
                    for snapshot in receipt_snapshots
                    if snapshot.exists and snapshot.get("input_id") == prior_id
                )
                prior_collection = receipt_data.get("record_collection", collection_name)
                if prior_collection not in {"inputs", "controls"}:
                    raise FirestoreStoreError("deduplicate")
                prior_ref = conversation_ref.collection(prior_collection).document(
                    document_id(cast(str, prior_id), prefix="input")
                )
                prior_snapshot = await prior_ref.get(transaction=transaction)
                if not prior_snapshot.exists:
                    raise FirestoreStoreError("deduplicate")
                for (receipt_ref, receipt_kind), snapshot in zip(
                    receipt_refs, receipt_snapshots, strict=True
                ):
                    if not snapshot.exists:
                        transaction.create(
                            receipt_ref,
                            {
                                "input_id": prior_id,
                                "record_collection": prior_collection,
                                "conversation_id": conversation_id,
                                "receipt_kind": receipt_kind,
                                "schema_version": 1,
                            },
                        )
                if participating:
                    transaction.set(conversation_ref, {"participating": True}, merge=True)
                return cast(dict[str, Any], prior_snapshot.to_dict()), False

            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            conversation_data = (
                conversation_snapshot.to_dict() if conversation_snapshot.exists else {}
            ) or {}
            stored_generation = conversation_data.get("conversation_generation")
            if stored_generation is not None and stored_generation != conversation_generation:
                raise FirestoreStoreError("accept_generation")
            next_seq = conversation_data.get("next_input_seq", 1)
            current_version = conversation_data.get("version", 0)
            if not isinstance(next_seq, int) or isinstance(next_seq, bool) or next_seq < 1:
                raise FirestoreStoreError("accept_sequence")
            if (
                not isinstance(current_version, int)
                or isinstance(current_version, bool)
                or current_version < 0
            ):
                raise FirestoreStoreError("accept_version")

            if sender_id is None:
                envelope: InputEnvelope | ControlEnvelope = InputEnvelope(
                    conversation_key,
                    next_seq,
                    input_id,
                    conversation_generation,
                    kind,
                    dict(payload),
                    created_at,
                )
            else:
                envelope = ControlEnvelope(
                    conversation_key,
                    next_seq,
                    input_id,
                    conversation_generation,
                    kind,
                    sender_id,
                    dict(payload),
                    created_at,
                )

            transaction.set(
                conversation_ref,
                {
                    "conversation_key": conversation_key,
                    "conversation_generation": conversation_generation,
                    "next_input_seq": next_seq + 1,
                    "allocation_state": "waiting",
                    **({"participating": True} if participating else {}),
                    **({"control_only": False} if sender_id is None else {}),
                    "version": current_version + 1,
                    "schema_version": 1,
                },
                merge=True,
            )
            record_data = encode_record(envelope)
            if sender_id is not None:
                record_data["authorization_state"] = "pending"
            sender_user = sender_id or payload.get("user")
            if isinstance(sender_user, str) and sender_user:
                record_data.update(
                    sender_lookup_state="pending",
                    sender_lookup_user=sender_user,
                    sender_lookup_attempts=0,
                    next_sender_lookup_at=created_at,
                )
            transaction.create(record_ref, record_data)
            if sender_id is not None:
                queue_ref = self._collection("control_queue").document(
                    document_id(conversation_key, input_id, prefix="control")
                )
                transaction.create(
                    queue_ref,
                    {
                        "conversation_key": conversation_key,
                        "input_id": input_id,
                        "conversation_generation": conversation_generation,
                        "seq": envelope.seq,
                        "kind": kind,
                        "sender_id": sender_id,
                        "authorization_state": "pending",
                        "sender_lookup_state": record_data.get("sender_lookup_state"),
                        "sender_lookup_user": record_data.get("sender_lookup_user"),
                        "sender_lookup_attempts": 0,
                        "next_sender_lookup_at": record_data.get("next_sender_lookup_at"),
                        "created_at": created_at,
                        "schema_version": 1,
                    },
                )
            for (receipt_ref, receipt_kind), _ in zip(receipt_refs, receipt_snapshots, strict=True):
                transaction.create(
                    receipt_ref,
                    {
                        "input_id": input_id,
                        "record_collection": collection_name,
                        "conversation_id": conversation_id,
                        "receipt_kind": receipt_kind,
                        "schema_version": 1,
                    },
                )
            return record_data, True

        try:
            data, created = await write(transaction)
            model_type = ControlEnvelope if "sender_id" in data else InputEnvelope
            return decode_record(data, model_type), created
        except FirestoreStoreError:
            raise
        except Exception as exc:
            raise convert_firestore_error("accept_input", exc) from None

    async def list_unfinished(
        self, conversation_key: str, *, after_seq: int = 0, limit: int = 100
    ) -> tuple[InputEnvelope, ...]:
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        conversation_ref = self._collection("conversations").document(
            self._conversation_id(conversation_key)
        )
        try:
            query = (
                conversation_ref.collection("inputs")
                .where(filter=FieldFilter("seq", ">", after_seq))
                .where(
                    filter=FieldFilter(
                        "delivery_state", "in", ["pending", "dispatching", "queued", "accepted"]
                    )
                )
                .order_by("seq")
                .limit(limit)
            )
            items = []
            async for snapshot in query.stream():
                items.append(decode_record(snapshot.to_dict() or {}, InputEnvelope))
            return tuple(items)
        except Exception as exc:
            raise convert_firestore_error("list_unfinished", exc) from None

    async def advance_input(
        self,
        *,
        conversation_key: str,
        input_id: str,
        owner: OwnershipToken,
        expected: InputDeliveryState,
        new_state: InputDeliveryState,
        dispatch_attempt_id: str | None = None,
    ) -> bool:
        conversation_id = self._conversation_id(conversation_key)
        conversation_ref = self._collection("conversations").document(conversation_id)
        input_ref = conversation_ref.collection("inputs").document(
            document_id(input_id, prefix="input")
        )
        owner_ref = self._collection("ownership").document(conversation_id)
        transaction = self.client.transaction()
        validate_state_transition(expected, new_state)
        if new_state is InputDeliveryState.DISPATCHING and not dispatch_attempt_id:
            raise ValueError("dispatch_attempt_id is required before dispatching")

        @async_transactional
        async def update(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            input_snapshot = await input_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            if (
                not owner_snapshot.exists
                or not input_snapshot.exists
                or not conversation_snapshot.exists
            ):
                return False
            if not ownership_matches(
                owner_snapshot.to_dict() or {},
                conversation_snapshot.to_dict() or {},
                owner,
                now=datetime.now(UTC),
            ):
                return False
            current = decode_record(input_snapshot.to_dict() or {}, InputEnvelope)
            if (
                current.conversation_generation != owner.conversation_generation
                or current.delivery_state is not expected
            ):
                return False
            conversation = conversation_snapshot.to_dict() or {}
            if conversation.get("reset_pending") and new_state in {
                InputDeliveryState.DISPATCHING,
                InputDeliveryState.QUEUED,
                InputDeliveryState.ACCEPTED,
            }:
                return False
            stop_seq = conversation.get("stop_seq", 0)
            if not isinstance(stop_seq, int) or isinstance(stop_seq, bool) or stop_seq < 0:
                return False
            if current.seq <= stop_seq and new_state in {
                InputDeliveryState.DISPATCHING,
                InputDeliveryState.QUEUED,
                InputDeliveryState.ACCEPTED,
            }:
                return False
            changes: dict[str, Any] = {"delivery_state": new_state.value}
            if dispatch_attempt_id is not None:
                changes["dispatch_attempt_id"] = dispatch_attempt_id
            transaction.update(input_ref, changes)
            return True

        try:
            return await update(transaction)
        except Exception as exc:
            raise convert_firestore_error("advance_input", exc) from None

    async def rebind_after_reset(
        self,
        *,
        conversation_key: str,
        previous_generation: int,
        reset_seq: int,
        owner: OwnershipToken,
    ) -> int:
        if (
            owner.conversation_key != conversation_key
            or owner.conversation_generation != previous_generation + 1
        ):
            raise ValueError("owner must represent the next generation")
        if reset_seq < 1:
            raise ValueError("reset_seq must be positive")
        conversation_ref = self._collection("conversations").document(
            self._conversation_id(conversation_key)
        )
        owner_ref = self._collection("ownership").document(self._conversation_id(conversation_key))
        rebound = 0
        cursor = reset_seq
        while True:
            page = await self.list_unfinished(conversation_key, after_seq=cursor, limit=100)
            if not page:
                return rebound
            cursor = page[-1].seq
            for envelope in page:
                if envelope.conversation_generation != previous_generation:
                    continue
                input_ref = conversation_ref.collection("inputs").document(
                    document_id(envelope.input_id, prefix="input")
                )
                transaction = self.client.transaction()

                @async_transactional
                async def rebind(
                    transaction: firestore.AsyncTransaction,
                    input_ref: firestore.AsyncDocumentReference = input_ref,
                ) -> bool:
                    owner_snapshot = await owner_ref.get(transaction=transaction)
                    conversation_snapshot = await conversation_ref.get(transaction=transaction)
                    input_snapshot = await input_ref.get(transaction=transaction)
                    if (
                        not owner_snapshot.exists
                        or not conversation_snapshot.exists
                        or not input_snapshot.exists
                    ):
                        return False
                    conversation = conversation_snapshot.to_dict() or {}
                    data = input_snapshot.to_dict() or {}
                    if (
                        not ownership_matches(
                            owner_snapshot.to_dict() or {},
                            conversation,
                            owner,
                            now=datetime.now(UTC),
                        )
                        or conversation.get("reset_pending")
                        or data.get("seq", 0) <= reset_seq
                        or data.get("conversation_generation") != previous_generation
                    ):
                        return False
                    state = data.get("delivery_state")
                    if state in {InputDeliveryState.PENDING.value, InputDeliveryState.QUEUED.value}:
                        if data.get("kind") == "message":
                            transaction.update(
                                input_ref,
                                {"conversation_generation": owner.conversation_generation},
                            )
                            return True
                        transaction.update(
                            input_ref,
                            {"delivery_state": InputDeliveryState.SUPPRESSED.value},
                        )
                        return False
                    if state in {
                        InputDeliveryState.DISPATCHING.value,
                        InputDeliveryState.ACCEPTED.value,
                    }:
                        transaction.update(
                            input_ref, {"delivery_state": InputDeliveryState.UNCERTAIN.value}
                        )
                    return False

                try:
                    if await rebind(transaction):
                        rebound += 1
                except Exception as exc:
                    raise convert_firestore_error("rebind_after_reset", exc) from None

    async def recover_dispatching(
        self,
        *,
        conversation_key: str,
        input_id: str,
        previous_owner_epoch: int,
        dispatch_attempt_id: str,
    ) -> bool:
        conversation_id = self._conversation_id(conversation_key)
        conversation_ref = self._collection("conversations").document(conversation_id)
        input_ref = conversation_ref.collection("inputs").document(
            document_id(input_id, prefix="input")
        )
        owner_ref = self._collection("ownership").document(conversation_id)
        transaction = self.client.transaction()

        @async_transactional
        async def recover(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            input_snapshot = await input_ref.get(transaction=transaction)
            if not input_snapshot.exists:
                return False
            current = decode_record(input_snapshot.to_dict() or {}, InputEnvelope)
            if (
                current.delivery_state is not InputDeliveryState.DISPATCHING
                or current.dispatch_attempt_id != dispatch_attempt_id
            ):
                return False
            if owner_snapshot.exists:
                saved_owner = decode_record(owner_snapshot.to_dict() or {}, OwnershipToken)
                if saved_owner.epoch == previous_owner_epoch:
                    return False
            transaction.update(input_ref, {"delivery_state": InputDeliveryState.UNCERTAIN.value})
            return True

        try:
            return await recover(transaction)
        except Exception as exc:
            raise convert_firestore_error("recover_dispatching", exc) from None

    async def record_query_start(
        self,
        *,
        conversation_key: str,
        input_id: str,
        owner: OwnershipToken,
        dispatch_attempt_id: str,
        sdk_session_id: str,
        now: datetime,
    ) -> bool:
        _validate_time(now, "now")
        if not dispatch_attempt_id or not sdk_session_id:
            raise ValueError("dispatch_attempt_id and sdk_session_id must be non-empty")
        conversation_ref = self._collection("conversations").document(
            self._conversation_id(conversation_key)
        )
        owner_ref = self._collection("ownership").document(self._conversation_id(conversation_key))
        input_ref = conversation_ref.collection("inputs").document(
            document_id(input_id, prefix="input")
        )
        transaction = self.client.transaction()

        @async_transactional
        async def record(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            input_snapshot = await input_ref.get(transaction=transaction)
            if (
                not owner_snapshot.exists
                or not conversation_snapshot.exists
                or not input_snapshot.exists
            ):
                return False
            conversation = conversation_snapshot.to_dict() or {}
            data = input_snapshot.to_dict() or {}
            if (
                not ownership_matches(owner_snapshot.to_dict() or {}, conversation, owner, now=now)
                or data.get("conversation_generation") != owner.conversation_generation
                or data.get("delivery_state") != InputDeliveryState.ACCEPTED.value
                or data.get("dispatch_attempt_id") != dispatch_attempt_id
            ):
                return False
            transaction.update(
                input_ref,
                {"sdk_session_id": sdk_session_id, "query_started_at": now},
            )
            transaction.update(
                conversation_ref,
                {
                    "sdk_session_id": sdk_session_id,
                    "version": conversation.get("version", 1) + 1,
                },
            )
            return True

        try:
            return await record(transaction)
        except Exception as exc:
            raise convert_firestore_error("record_query_start", exc) from None

    async def list_sender_enrichment_due(
        self, *, now: datetime, limit: int = 100
    ) -> tuple[Mapping[str, Any], ...]:
        _validate_time(now, "now")
        if limit < 1:
            raise ValueError("limit must be positive")
        try:
            records: list[Mapping[str, Any]] = []
            query = (
                self.client.collection_group("inputs")
                .where(filter=FieldFilter("next_sender_lookup_at", "<=", now))
                .limit(limit)
            )
            async for snapshot in query.stream():
                data = snapshot.to_dict() or {}
                if data.get("sender_lookup_state") == "pending":
                    records.append({**data, "record_collection": "inputs"})
            control_query = (
                self._collection("control_queue")
                .where(filter=FieldFilter("next_sender_lookup_at", "<=", now))
                .limit(limit)
            )
            async for snapshot in control_query.stream():
                queue_record = snapshot.to_dict() or {}
                if queue_record.get("sender_lookup_state") != "pending":
                    continue
                conversation_key = queue_record.get("conversation_key")
                input_id = queue_record.get("input_id")
                if not isinstance(conversation_key, str) or not isinstance(input_id, str):
                    continue
                control = await self._control_ref(conversation_key, input_id).get()
                if control.exists:
                    records.append({**(control.to_dict() or {}), "record_collection": "controls"})
            records.sort(key=lambda record: record.get("next_sender_lookup_at", now))
            return tuple(records[:limit])
        except Exception as exc:
            raise convert_firestore_error("list_sender_enrichment_due", exc) from None

    async def record_sender_enrichment(
        self,
        *,
        conversation_key: str,
        input_id: str,
        record_collection: str,
        sender_info: Mapping[str, Any] | None,
        retry_at: datetime,
        now: datetime,
    ) -> bool:
        _validate_time(now, "now")
        _validate_time(retry_at, "retry_at")
        if record_collection not in {"inputs", "controls"}:
            raise ValueError("record_collection must be inputs or controls")
        conversation_ref = self._collection("conversations").document(
            self._conversation_id(conversation_key)
        )
        record_ref = (
            self._control_ref(conversation_key, input_id)
            if record_collection == "controls"
            else conversation_ref.collection("inputs").document(
                document_id(input_id, prefix="input")
            )
        )
        queue_ref = self._control_queue_ref(conversation_key, input_id)
        transaction = self.client.transaction()

        @async_transactional
        async def update(transaction: firestore.AsyncTransaction) -> bool:
            snapshot = await record_ref.get(transaction=transaction)
            queue_snapshot = (
                await queue_ref.get(transaction=transaction)
                if record_collection == "controls"
                else None
            )
            if not snapshot.exists:
                return False
            data = snapshot.to_dict() or {}
            if data.get("conversation_key") != conversation_key:
                return False
            if data.get("sender_lookup_state") != "pending":
                return False
            attempts = data.get("sender_lookup_attempts", 0)
            if not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 0:
                return False
            update_data: dict[str, Any] = {
                "sender_lookup_attempts": attempts + 1,
                "updated_at": now,
            }
            if sender_info is None:
                update_data.update(
                    sender_lookup_state="pending",
                    next_sender_lookup_at=retry_at,
                )
            else:
                envelope = data.get("payload")
                if not isinstance(envelope, Mapping):
                    return False
                update_data.update(
                    sender_lookup_state="resolved",
                    sender_info=dict(sender_info),
                    payload={**envelope, "sender": dict(sender_info)},
                    next_sender_lookup_at=firestore.DELETE_FIELD,
                )
            transaction.update(record_ref, update_data)
            if queue_snapshot is not None and queue_snapshot.exists:
                transaction.update(
                    queue_ref,
                    {
                        "sender_lookup_state": update_data["sender_lookup_state"],
                        "sender_lookup_attempts": attempts + 1,
                        "next_sender_lookup_at": update_data.get(
                            "next_sender_lookup_at", firestore.DELETE_FIELD
                        ),
                    },
                )
            return True

        try:
            return await update(transaction)
        except Exception as exc:
            raise convert_firestore_error("record_sender_enrichment", exc) from None

    async def list_pending_controls(self, *, limit: int = 100) -> tuple[Mapping[str, Any], ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        query = (
            self._collection("control_queue")
            .where(filter=FieldFilter("authorization_state", "==", "pending"))
            .limit(limit)
        )
        try:
            records: list[Mapping[str, Any]] = []
            async for snapshot in query.stream():
                queue_record = snapshot.to_dict() or {}
                conversation_key = queue_record.get("conversation_key")
                input_id = queue_record.get("input_id")
                if not isinstance(conversation_key, str) or not isinstance(input_id, str):
                    continue
                control = await self._control_ref(conversation_key, input_id).get()
                if control.exists:
                    records.append({**(control.to_dict() or {}), **queue_record})
            return tuple(records)
        except Exception as exc:
            raise convert_firestore_error("list_pending_controls", exc) from None

    async def list_authorized_controls(
        self, conversation_key: str, *, limit: int = 100
    ) -> tuple[Mapping[str, Any], ...]:
        """Authorized controls of one conversation that its owner has not handled yet."""
        if limit < 1:
            raise ValueError("limit must be positive")
        query = (
            self._collection("conversations")
            .document(self._conversation_id(conversation_key))
            .collection("controls")
            .where(filter=FieldFilter("authorization_state", "==", "authorized"))
            .limit(limit)
        )
        try:
            records = [snapshot.to_dict() or {} async for snapshot in query.stream()]
        except Exception as exc:
            raise convert_firestore_error("list_authorized_controls", exc) from None
        return tuple(sorted(records, key=lambda item: item.get("seq", 0)))

    async def mark_control_handled(
        self, *, conversation_key: str, input_id: str, owner: OwnershipToken, now: datetime
    ) -> bool:
        """Close an authorized control once its stop/reset effect is complete."""
        _validate_time(now, "now")
        conversation_ref = self._collection("conversations").document(
            self._conversation_id(conversation_key)
        )
        owner_ref = self._collection("ownership").document(self._conversation_id(conversation_key))
        control_ref = self._control_ref(conversation_key, input_id)
        queue_ref = self._control_queue_ref(conversation_key, input_id)
        transaction = self.client.transaction()

        @async_transactional
        async def handled(transaction: firestore.AsyncTransaction) -> bool:
            owner_snapshot = await owner_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            control_snapshot = await control_ref.get(transaction=transaction)
            queue_snapshot = await queue_ref.get(transaction=transaction)
            if not (owner_snapshot.exists and conversation_snapshot.exists):
                return False
            if (
                not control_snapshot.exists
                or (control_snapshot.to_dict() or {}).get("authorization_state") != "authorized"
            ):
                return False
            if not ownership_matches(
                owner_snapshot.to_dict() or {},
                conversation_snapshot.to_dict() or {},
                owner,
                now=now,
            ):
                return False
            transaction.update(control_ref, {"authorization_state": "handled", "handled_at": now})
            if queue_snapshot.exists:
                transaction.update(queue_ref, {"authorization_state": "handled"})
            return True

        try:
            return await handled(transaction)
        except Exception as exc:
            raise convert_firestore_error("mark_control_handled", exc) from None

    async def resolve_control_authorization(
        self,
        *,
        conversation_key: str,
        input_id: str,
        authorized: bool,
        now: datetime,
    ) -> bool:
        _validate_time(now, "now")
        conversation_ref = self._collection("conversations").document(
            self._conversation_id(conversation_key)
        )
        control_ref = conversation_ref.collection("controls").document(
            document_id(input_id, prefix="input")
        )
        queue_ref = self._control_queue_ref(conversation_key, input_id)
        transaction = self.client.transaction()

        @async_transactional
        async def resolve(transaction: firestore.AsyncTransaction) -> bool:
            control_snapshot = await control_ref.get(transaction=transaction)
            conversation_snapshot = await conversation_ref.get(transaction=transaction)
            queue_snapshot = await queue_ref.get(transaction=transaction)
            if (
                not control_snapshot.exists
                or not conversation_snapshot.exists
                or not queue_snapshot.exists
            ):
                return False
            control = control_snapshot.to_dict() or {}
            conversation = conversation_snapshot.to_dict() or {}
            if control.get("authorization_state") != "pending":
                return False
            if control.get("conversation_generation") != conversation.get(
                "conversation_generation"
            ):
                transaction.update(
                    control_ref,
                    {
                        "authorization_state": "stale",
                        "authorized_at": now,
                    },
                )
                transaction.update(queue_ref, {"authorization_state": "stale"})
                return True
            if authorized and control.get("kind") in {"stop_candidate", "reset_candidate"}:
                seq = control.get("seq")
                stop_seq = conversation.get("stop_seq", 0)
                if (
                    not isinstance(seq, int)
                    or isinstance(seq, bool)
                    or not isinstance(stop_seq, int)
                    or isinstance(stop_seq, bool)
                    or seq <= stop_seq
                ):
                    return False
                control_patch: dict[str, Any] = {
                    "stop_seq": seq,
                    "stop_requested_at": now,
                    "control_only": conversation.get("turn_status", "idle") != "running",
                }
                if control.get("kind") == "reset_candidate":
                    control_patch.update(reset_pending=True, reset_seq=seq)
                transaction.update(conversation_ref, control_patch)
            transaction.update(
                control_ref,
                {
                    "authorization_state": "authorized" if authorized else "filtered",
                    "authorized_at": now,
                },
            )
            transaction.update(
                queue_ref,
                {"authorization_state": "authorized" if authorized else "filtered"},
            )
            return True

        try:
            return await resolve(transaction)
        except Exception as exc:
            raise convert_firestore_error("resolve_control_authorization", exc) from None

    def _control_ref(self, conversation_key: str, input_id: str):
        return (
            self._collection("conversations")
            .document(self._conversation_id(conversation_key))
            .collection("controls")
            .document(document_id(input_id, prefix="input"))
        )

    def _control_queue_ref(self, conversation_key: str, input_id: str):
        return self._collection("control_queue").document(
            document_id(conversation_key, input_id, prefix="control")
        )


def _validate_time(value: datetime, field_name: str) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be UTC-aware")


def inbox_store(
    client: firestore.AsyncClient, *, collection_prefix: str = "silk-agent"
) -> InboxStore:
    return FirestoreInboxStore(client, collection_prefix=collection_prefix)
