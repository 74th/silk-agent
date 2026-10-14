"""Atomic per-user rolling-minute rate limits shared by Worker processes."""

from __future__ import annotations

from datetime import datetime, timedelta

from google.cloud import firestore
from google.cloud.firestore_v1.async_transaction import async_transactional

from silk_agent.core.distributed_ports import UserRateLimitStore
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import document_id


class FirestoreUserRateLimitStore:
    def __init__(self, client: firestore.AsyncClient, *, collection_prefix: str = "silk-agent"):
        if not collection_prefix or "/" in collection_prefix:
            raise ValueError("collection_prefix must be non-empty and cannot contain '/'")
        self.client = client
        self.collection_prefix = collection_prefix

    async def allow_and_record(
        self, *, user_key: str, input_id: str, now: datetime, per_minute: int
    ) -> bool:
        if not user_key or not input_id:
            raise ValueError("user_key and input_id must be non-empty")
        _validate_utc(now)
        if per_minute < 0:
            raise ValueError("per_minute must be non-negative")
        if per_minute == 0:
            return True
        ref = self.client.collection(f"{self.collection_prefix}_user_rate_limits").document(
            document_id(user_key, prefix="rate")
        )
        transaction = self.client.transaction()

        @async_transactional
        async def check(transaction: firestore.AsyncTransaction) -> bool:
            snapshot = await ref.get(transaction=transaction)
            data = snapshot.to_dict() or {}
            hits = data.get("hits", [])
            if not isinstance(hits, list):
                hits = []
            cutoff = now - timedelta(seconds=60)
            recent = [
                hit
                for hit in hits
                if (
                    isinstance(hit, dict)
                    and isinstance(hit.get("at"), datetime)
                    and hit["at"].tzinfo is not None
                    and hit["at"] >= cutoff
                )
            ]
            if any(hit.get("input_id") == input_id for hit in recent):
                return True
            if len(recent) >= per_minute:
                return False
            transaction.set(
                ref,
                {
                    "hits": [*recent, {"input_id": input_id, "at": now}],
                    "updated_at": now,
                    "schema_version": 1,
                },
            )
            return True

        try:
            return await check(transaction)
        except Exception as exc:
            raise convert_firestore_error("check_user_rate_limit", exc) from None


def _validate_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("now must be timezone-aware UTC")


def user_rate_limit_store(
    client: firestore.AsyncClient, *, collection_prefix: str = "silk-agent"
) -> UserRateLimitStore:
    return FirestoreUserRateLimitStore(client, collection_prefix=collection_prefix)
