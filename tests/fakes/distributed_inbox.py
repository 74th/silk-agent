from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any

from silk_agent.core.distributed_models import (
    ControlEnvelope,
    InputDeliveryState,
    InputEnvelope,
    OwnershipToken,
    validate_state_transition,
)
from silk_agent.core.distributed_ports import InboxStore


class FakeInboxStore:
    """Small shared inbox fake that checks the same CAS contract as the port."""

    def __init__(self) -> None:
        self.inputs: dict[tuple[str, str], InputEnvelope] = {}
        self.controls: dict[tuple[str, str], ControlEnvelope] = {}
        self.owners: dict[str, OwnershipToken] = {}
        self.receipts: dict[tuple[str, ...], tuple[str, str, str]] = {}
        self.conversations: dict[str, dict[str, Any]] = {}
        self.sender_enrichment: dict[tuple[str, str], dict[str, Any]] = {}
        self.control_authorization: dict[tuple[str, str], str] = {}

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
        key = (conversation_key, input_id)
        existing = self.inputs.get(key)
        if existing is not None:
            return existing
        seq = self._next_seq(conversation_key)
        envelope = InputEnvelope(
            conversation_key,
            seq,
            input_id,
            conversation_generation,
            kind,
            dict(payload),
            created_at,
        )
        self.inputs[key] = envelope
        user = payload.get("user")
        if isinstance(user, str) and user:
            self.sender_enrichment[key] = {
                "sender_lookup_state": "pending",
                "sender_lookup_user": user,
                "sender_lookup_attempts": 0,
                "next_sender_lookup_at": created_at,
            }
        return envelope

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
        key = (conversation_key, input_id)
        existing = self.controls.get(key)
        if existing is not None:
            return existing
        envelope = ControlEnvelope(
            conversation_key,
            self._next_seq(conversation_key),
            input_id,
            conversation_generation,
            kind,
            sender_id,
            dict(payload),
            created_at,
        )
        self.controls[key] = envelope
        self.control_authorization[key] = "pending"
        self.sender_enrichment[key] = {
            "sender_lookup_state": "pending",
            "sender_lookup_user": sender_id,
            "sender_lookup_attempts": 0,
            "next_sender_lookup_at": created_at,
        }
        return envelope

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
        event_receipt = ("event", workspace, event_id)
        message_receipt = ("message", workspace, team, channel, message_ts)
        prior = {
            self.receipts[key] for key in (event_receipt, message_receipt) if key in self.receipts
        }
        if len(prior) > 1:
            raise ValueError("receipt conflict")
        if prior:
            record_collection, prior_conversation, prior_id = next(iter(prior))
            key = (prior_conversation, prior_id)
            record = (self.controls if record_collection == "controls" else self.inputs).get(key)
            if record is None:
                raise ValueError("receipt has no input record")
            receipt_value = (record_collection, prior_conversation, prior_id)
            self.receipts[event_receipt] = receipt_value
            self.receipts[message_receipt] = receipt_value
            if participating:
                self.conversations.setdefault(conversation_key, {})["participating"] = True
            return record
        if sender_id is None:
            record = await self.accept_input(
                conversation_key=conversation_key,
                input_id=input_id,
                conversation_generation=conversation_generation,
                kind=kind,
                payload=payload,
                created_at=created_at,
            )
            record_collection = "inputs"
        else:
            record = await self.accept_control(
                conversation_key=conversation_key,
                input_id=input_id,
                conversation_generation=conversation_generation,
                kind=kind,
                sender_id=sender_id,
                payload=payload,
                created_at=created_at,
            )
            record_collection = "controls"
        receipt_value = (record_collection, conversation_key, input_id)
        if participating:
            self.conversations.setdefault(conversation_key, {})["participating"] = True
        if sender_id is None:
            self.conversations.setdefault(conversation_key, {})["control_only"] = False
        self.receipts[event_receipt] = receipt_value
        self.receipts[message_receipt] = receipt_value
        return record

    async def list_held(self, conversation_key: str, *, limit: int = 50):
        return tuple(
            sorted(
                (
                    item
                    for (key, _), item in self.inputs.items()
                    if key == conversation_key and item.delivery_state is InputDeliveryState.HELD
                ),
                key=lambda item: item.seq,
            )
        )[:limit]

    async def list_unfinished(
        self,
        conversation_key: str,
        *,
        after_seq: int = 0,
        limit: int = 100,
        states=None,
    ) -> Sequence[InputEnvelope]:
        terminal = {
            InputDeliveryState.COMPLETED,
            InputDeliveryState.BLOCKED,
            InputDeliveryState.CANCELLED,
            InputDeliveryState.SUPPRESSED,
            InputDeliveryState.UNCERTAIN,
            InputDeliveryState.HELD,  # the real store never lists held inputs
        }
        return tuple(
            sorted(
                (
                    item
                    for (key, _), item in self.inputs.items()
                    if key == conversation_key
                    and item.seq > after_seq
                    and item.delivery_state not in terminal
                    and (states is None or item.delivery_state in states)
                ),
                key=lambda item: item.seq,
            )[:limit]
        )

    async def list_sender_enrichment_due(self, *, now, limit=100):
        due = []
        for (conversation_key, input_id), state in self.sender_enrichment.items():
            if state["sender_lookup_state"] != "pending" or state["next_sender_lookup_at"] > now:
                continue
            record_collection = "inputs"
            envelope = self.inputs.get((conversation_key, input_id))
            if envelope is None:
                record_collection = "controls"
                envelope = self.controls.get((conversation_key, input_id))
            if envelope is None:
                continue
            due.append(
                {
                    **envelope.to_dict(),
                    **state,
                    "conversation_key": conversation_key,
                    "input_id": input_id,
                    "record_collection": record_collection,
                }
            )
        due.sort(key=lambda item: item["next_sender_lookup_at"])
        return due[:limit]

    async def record_sender_enrichment(
        self,
        *,
        conversation_key,
        input_id,
        record_collection,
        sender_info,
        retry_at,
        now,
        give_up=False,
    ):
        key = (conversation_key, input_id)
        state = self.sender_enrichment.get(key)
        envelope = (
            self.controls.get(key) if record_collection == "controls" else self.inputs.get(key)
        )
        if state is None or envelope is None or state["sender_lookup_state"] != "pending":
            return False
        state["sender_lookup_attempts"] += 1
        state["updated_at"] = now
        if sender_info is None and give_up:
            state["sender_lookup_state"] = "failed"
            state.pop("next_sender_lookup_at", None)
            if record_collection == "controls":
                self.control_authorization[key] = "filtered"
            elif envelope.delivery_state is InputDeliveryState.PENDING:
                self.inputs[key] = replace(envelope, delivery_state=InputDeliveryState.BLOCKED)
        elif sender_info is None:
            state["next_sender_lookup_at"] = retry_at
        else:
            state["sender_lookup_state"] = "resolved"
            state.pop("next_sender_lookup_at", None)
            updated = replace(envelope, payload={**envelope.payload, "sender": dict(sender_info)})
            if record_collection == "controls":
                self.controls[key] = updated
            else:
                self.inputs[key] = updated
        return True

    async def list_pending_controls(self, *, limit=100):
        records = []
        for key, envelope in self.controls.items():
            if self.control_authorization.get(key) != "pending":
                continue
            records.append(
                {
                    **envelope.to_dict(),
                    **self.sender_enrichment.get(key, {}),
                    "conversation_key": key[0],
                    "input_id": key[1],
                    "authorization_state": "pending",
                }
            )
        return records[:limit]

    async def list_authorized_controls(self, conversation_key, *, limit=100):
        records = []
        for (key, input_id), envelope in self.controls.items():
            if key == conversation_key and self.control_authorization.get((key, input_id)) == (
                "authorized"
            ):
                records.append(
                    {
                        "conversation_key": key,
                        "input_id": input_id,
                        "kind": envelope.kind,
                        "seq": envelope.seq,
                        "conversation_generation": envelope.conversation_generation,
                    }
                )
        return tuple(sorted(records, key=lambda item: item["seq"]))[:limit]

    async def mark_control_handled(self, *, conversation_key, input_id, owner, now):
        del owner, now
        key = (conversation_key, input_id)
        if self.control_authorization.get(key) != "authorized":
            return False
        self.control_authorization[key] = "handled"
        return True

    async def resolve_control_authorization(self, *, conversation_key, input_id, authorized, now):
        del now
        key = (conversation_key, input_id)
        record = self.controls.get(key)
        if record is None or self.control_authorization.get(key) != "pending":
            return False
        if authorized and record.kind in {"stop_candidate", "reset_candidate"}:
            conversation = self.conversations.setdefault(conversation_key, {})
            if record.seq <= conversation.get("stop_seq", 0):
                return False
            conversation["stop_seq"] = record.seq
            conversation["control_only"] = conversation.get("turn_status", "idle") != "running"
            if record.kind == "reset_candidate":
                conversation.update(reset_pending=True, reset_seq=record.seq)
        self.control_authorization[key] = "authorized" if authorized else "filtered"
        return True

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
        key = (conversation_key, input_id)
        current = self.inputs.get(key)
        if current is None or self.owners.get(conversation_key) != owner:
            return False
        if current.delivery_state is not expected:
            return False
        conversation = self.conversations.get(conversation_key, {})
        if conversation.get("reset_pending") and new_state in {
            InputDeliveryState.DISPATCHING,
            InputDeliveryState.QUEUED,
            InputDeliveryState.ACCEPTED,
        }:
            return False
        stop_seq = conversation.get("stop_seq", 0)
        if current.seq <= stop_seq and new_state in {
            InputDeliveryState.DISPATCHING,
            InputDeliveryState.QUEUED,
            InputDeliveryState.ACCEPTED,
        }:
            return False
        if new_state is InputDeliveryState.DISPATCHING and not dispatch_attempt_id:
            raise ValueError("dispatch_attempt_id is required before dispatching")
        validate_state_transition(expected, new_state)
        self.inputs[key] = replace(
            current,
            delivery_state=new_state,
            dispatch_attempt_id=dispatch_attempt_id or current.dispatch_attempt_id,
        )
        return True

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
        count = 0
        for key, current in tuple(self.inputs.items()):
            if (
                key[0] != conversation_key
                or current.seq <= reset_seq
                or current.conversation_generation != previous_generation
            ):
                continue
            if current.delivery_state in {InputDeliveryState.PENDING, InputDeliveryState.QUEUED}:
                if current.kind == "message":
                    self.inputs[key] = replace(
                        current, conversation_generation=owner.conversation_generation
                    )
                    count += 1
                else:
                    self.inputs[key] = replace(
                        current, delivery_state=InputDeliveryState.SUPPRESSED
                    )
            elif current.delivery_state in {
                InputDeliveryState.DISPATCHING,
                InputDeliveryState.ACCEPTED,
            }:
                self.inputs[key] = replace(current, delivery_state=InputDeliveryState.UNCERTAIN)
        return count

    async def recover_dispatching(
        self,
        *,
        conversation_key: str,
        input_id: str,
        previous_owner_epoch: int,
        dispatch_attempt_id: str,
    ) -> bool:
        key = (conversation_key, input_id)
        current = self.inputs.get(key)
        owner = self.owners.get(conversation_key)
        if current is None or current.delivery_state is not InputDeliveryState.DISPATCHING:
            return False
        if current.dispatch_attempt_id != dispatch_attempt_id:
            return False
        if owner is not None and owner.epoch == previous_owner_epoch:
            return False
        validate_state_transition(InputDeliveryState.DISPATCHING, InputDeliveryState.UNCERTAIN)
        self.inputs[key] = replace(current, delivery_state=InputDeliveryState.UNCERTAIN)
        return True

    async def record_query_start(
        self,
        *,
        conversation_key,
        input_id,
        owner,
        dispatch_attempt_id,
        sdk_session_id,
        now,
    ):
        key = (conversation_key, input_id)
        current = self.inputs.get(key)
        if (
            current is None
            or self.owners.get(conversation_key) != owner
            or current.delivery_state is not InputDeliveryState.ACCEPTED
            or current.dispatch_attempt_id != dispatch_attempt_id
        ):
            return False
        self.inputs[key] = replace(
            current,
            payload={
                **current.payload,
                "sdk_session_id": sdk_session_id,
                "query_started_at": now.isoformat(),
            },
        )
        return True

    def _next_seq(self, conversation_key: str) -> int:
        input_seqs = [
            envelope.seq for (key, _), envelope in self.inputs.items() if key == conversation_key
        ]
        control_seqs = [
            envelope.seq for (key, _), envelope in self.controls.items() if key == conversation_key
        ]
        return max((*input_seqs, *control_seqs), default=0) + 1


def inbox_store_contract(fake: FakeInboxStore) -> InboxStore:
    """A typed assignment makes pyrefly verify this fake against the full port."""
    return fake


def _yielding(function):
    """Every store call yields to the event loop once, as a real RPC does.

    Without this, an in-memory fake never lets another task run in the middle of an operation,
    so races between a reader scan and an in-flight hand-off cannot show up in tests.
    """

    @functools.wraps(function)
    async def wrapper(*args, **kwargs):
        await asyncio.sleep(0)
        return await function(*args, **kwargs)

    return wrapper


def make_yielding(cls) -> None:
    for name, function in list(vars(cls).items()):
        if inspect.iscoroutinefunction(function) and not name.startswith("_"):
            setattr(cls, name, _yielding(function))


make_yielding(FakeInboxStore)
