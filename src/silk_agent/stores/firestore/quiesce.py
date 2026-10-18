"""Firestore mailbox for Control -> Worker "stop your SDK for this ownership" requests."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from silk_agent.core.distributed_models import OwnershipToken
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import document_id
from silk_agent.stores.firestore.retention import RetentionPolicy, quiesce_expiry


class FirestoreQuiesceMailbox:
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

    def _ref(self, token: OwnershipToken):
        request_id = document_id(token.conversation_key, str(token.epoch), prefix="quiesce")
        return self.client.collection(f"{self.collection_prefix}_quiesce_requests").document(
            request_id
        )

    async def request(self, token: OwnershipToken, *, now: datetime) -> Mapping[str, Any]:
        """Create (or return the existing) request for this exact ownership epoch."""
        ref = self._ref(token)
        try:
            snapshot = await ref.get()
            if snapshot.exists:
                return snapshot.to_dict() or {}
            record = {
                "state": "requested",
                "token": token.to_dict(),
                "worker_id": token.worker_id,
                "worker_incarnation": token.worker_incarnation,
                "requested_at": now.isoformat(),
            }
            await ref.create(record)
            return record
        except Exception as exc:
            raise convert_firestore_error("request_quiesce", exc) from None

    async def get(self, token: OwnershipToken) -> Mapping[str, Any] | None:
        try:
            snapshot = await self._ref(token).get()
            return snapshot.to_dict() if snapshot.exists else None
        except Exception as exc:
            raise convert_firestore_error("get_quiesce", exc) from None

    async def list_requested(
        self, *, worker_id: str, incarnation: str
    ) -> tuple[OwnershipToken, ...]:
        query = (
            self.client.collection(f"{self.collection_prefix}_quiesce_requests")
            .where(filter=FieldFilter("worker_incarnation", "==", incarnation))
            .where(filter=FieldFilter("state", "==", "requested"))
            .limit(100)
        )
        try:
            tokens = []
            async for snapshot in query.stream():
                data = snapshot.to_dict() or {}
                if data.get("worker_id") == worker_id:
                    tokens.append(OwnershipToken.from_dict(data["token"]))
            return tuple(tokens)
        except Exception as exc:
            raise convert_firestore_error("list_quiesce_requests", exc) from None

    async def answer(
        self, token: OwnershipToken, *, evidence: Mapping[str, object], now: datetime
    ) -> None:
        try:
            patch: dict[str, Any] = {
                "state": "answered",
                "evidence": dict(evidence),
                "answered_at": now.isoformat(),
            }
            expiry = quiesce_expiry("answered", now, retention=self.retention.terminal)
            if expiry is not None:
                patch["expire_at"] = expiry
            await self._ref(token).update(patch)
        except Exception as exc:
            raise convert_firestore_error("answer_quiesce", exc) from None
