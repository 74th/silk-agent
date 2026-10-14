"""Stable, opaque Firestore document identifiers."""

from __future__ import annotations

import hashlib
import json
import re

_PREFIX_RE = re.compile(r"^[A-Za-z0-9_-]{1,40}$")


def document_id(*parts: str, prefix: str | None = None) -> str:
    """Hash structured identifiers so user-controlled keys never become document paths."""
    if not parts or any(not isinstance(part, str) or not part for part in parts):
        raise ValueError("document ID parts must be non-empty strings")
    if prefix is not None and _PREFIX_RE.fullmatch(prefix) is None:
        raise ValueError("document ID prefix must contain only letters, digits, '_' or '-'")
    payload = json.dumps(list(parts), ensure_ascii=False, separators=(",", ":")).encode()
    digest = hashlib.sha256(payload).hexdigest()
    return f"{prefix}_{digest}" if prefix else digest


def conversation_document_id(conversation_key: str, *, prefix: str = "conversation") -> str:
    return document_id(conversation_key, prefix=prefix)


def worker_incarnation(pod_uid: str, startup_uuid: str) -> str:
    """Build a process identity that changes after any Pod process restart."""
    if not pod_uid or not startup_uuid:
        raise ValueError("pod_uid and startup_uuid must be non-empty")
    return json.dumps([pod_uid, startup_uuid], ensure_ascii=False, separators=(",", ":"))


def receipt_document_id(
    kind: str, team: str, channel: str, message_ts: str, *, workspace: str
) -> str:
    """Receipt key includes workspace and channel, even when Slack timestamps match."""
    return document_id(kind, workspace, team, channel, message_ts, prefix="receipt")
