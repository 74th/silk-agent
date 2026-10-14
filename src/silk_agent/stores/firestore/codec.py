"""Versioned conversion between distributed records and Firestore document values."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol


class FirestoreRecord(Protocol):
    def to_dict(self) -> dict[str, Any]: ...


def encode_record(record: FirestoreRecord) -> dict[str, Any]:
    """Encode a model using Firestore native timestamps and plain mapping values."""
    return _to_firestore_value(record.to_dict())


def decode_record[T](data: Mapping[str, Any], record_type: type[T]) -> T:
    """Decode values and delegate enum/schema validation to the record model."""
    from_dict = getattr(record_type, "from_dict", None)
    if not callable(from_dict):
        raise TypeError("record_type must provide from_dict")
    return from_dict(_from_firestore_value(data))


def _to_firestore_value(value: Any, key: str | None = None) -> Any:
    if isinstance(value, Mapping):
        return {str(name): _to_firestore_value(item, str(name)) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_firestore_value(item) for item in value]
    if isinstance(value, str) and key is not None and key.endswith("_at"):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return value
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"{key} must be timezone-aware")
        if parsed.utcoffset() != timedelta(0):
            raise ValueError(f"{key} must be UTC")
        return parsed.astimezone(UTC)
    return value


def _from_firestore_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Firestore timestamps must be timezone-aware")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _from_firestore_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_from_firestore_value(item) for item in value]
    return value
