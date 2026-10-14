"""Firestore exception conversion that never copies request or document contents."""

from __future__ import annotations

from typing import Any

_SAFE_STATUS_NAMES = frozenset(
    {
        "CANCELLED",
        "UNKNOWN",
        "INVALID_ARGUMENT",
        "DEADLINE_EXCEEDED",
        "NOT_FOUND",
        "ALREADY_EXISTS",
        "PERMISSION_DENIED",
        "RESOURCE_EXHAUSTED",
        "FAILED_PRECONDITION",
        "ABORTED",
        "OUT_OF_RANGE",
        "UNAUTHENTICATED",
        "INTERNAL",
        "UNAVAILABLE",
        "DATA_LOSS",
    }
)


class FirestoreStoreError(RuntimeError):
    def __init__(self, operation: str, status: str | None = None) -> None:
        self.operation = operation
        self.status = status
        suffix = f" ({status})" if status else ""
        super().__init__(f"Firestore {operation} failed{suffix}")


def convert_firestore_error(operation: str, error: BaseException) -> FirestoreStoreError:
    """Return a safe error with only the operation and a known status code."""
    if not operation or not operation.replace("_", "").isalnum():
        raise ValueError("operation must be an alphanumeric identifier")
    status = _status_name(getattr(error, "code", None))
    return FirestoreStoreError(operation, status)


def _status_name(code: Any) -> str | None:
    if callable(code):
        try:
            code = code()
        except Exception:
            return None
    name = getattr(code, "name", None)
    if isinstance(name, str) and name in _SAFE_STATUS_NAMES:
        return name
    if isinstance(code, str) and code in _SAFE_STATUS_NAMES:
        return code
    return None
