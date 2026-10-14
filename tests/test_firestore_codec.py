from datetime import UTC, datetime

import pytest
from google.api_core.exceptions import NotFound

from silk_agent.core.distributed_models import InputEnvelope, WorkerRecord
from silk_agent.stores.firestore.codec import decode_record, encode_record
from silk_agent.stores.firestore.errors import convert_firestore_error
from silk_agent.stores.firestore.ids import (
    conversation_document_id,
    document_id,
    receipt_document_id,
    worker_incarnation,
)

NOW = datetime(2026, 10, 14, 3, 0, tzinfo=UTC)


def test_document_ids_are_stable_and_opaque():
    first = conversation_document_id("workspace/C123/channel/C9/thread/123.456")

    assert first == conversation_document_id("workspace/C123/channel/C9/thread/123.456")
    assert "/" not in first
    assert first.startswith("conversation_")


def test_receipt_ids_include_workspace_even_for_same_slack_timestamp():
    first = receipt_document_id("message", "T1", "C1", "123.456", workspace="W1")
    second = receipt_document_id("message", "T1", "C1", "123.456", workspace="W2")

    assert first != second


def test_document_ids_validate_parts_and_prefix():
    with pytest.raises(ValueError, match="non-empty strings"):
        document_id("conversation", "")
    with pytest.raises(ValueError, match="prefix"):
        document_id("conversation", prefix="bad/prefix")


def test_worker_incarnation_changes_when_process_restarts_in_same_pod():
    first = worker_incarnation("pod-uid", "startup-uuid-1")
    restarted = worker_incarnation("pod-uid", "startup-uuid-2")

    assert first != restarted
    assert worker_incarnation("pod-uid", "startup-uuid-1") == first


def test_worker_incarnation_requires_both_identity_parts():
    with pytest.raises(ValueError, match="non-empty"):
        worker_incarnation("pod-uid", "")


@pytest.mark.parametrize(
    "record_type,record",
    [
        (
            InputEnvelope,
            InputEnvelope("conversation", 1, "input-1", 1, "message", {"text": "safe"}, NOW),
        ),
        (WorkerRecord, WorkerRecord("worker-1", "incarnation-1", 4, 1, 1, False, NOW)),
    ],
)
def test_firestore_codec_round_trip_with_native_timestamps(record_type, record):
    encoded = encode_record(record)

    assert isinstance(encoded.get("created_at", encoded.get("heartbeat_at")), datetime)
    assert decode_record(encoded, record_type) == record


def test_firestore_codec_rejects_unknown_enum_value():
    record = InputEnvelope("conversation", 1, "input-1", 1, "message", {}, NOW)
    encoded = encode_record(record)
    encoded["delivery_state"] = "not-known"

    with pytest.raises(ValueError):
        decode_record(encoded, InputEnvelope)


def test_firestore_codec_rejects_naive_timestamps():
    with pytest.raises(ValueError, match="timezone-aware"):
        encode_record(
            InputEnvelope("conversation", 1, "input-1", 1, "message", {}, datetime(2026, 10, 14))
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        decode_record(
            {"heartbeat_at": datetime(2026, 10, 14)},
            WorkerRecord,
        )


def test_firestore_errors_exclude_exception_text_and_credentials():
    safe = convert_firestore_error(
        "transaction", NotFound("document contains secret-token-and-user-text")
    )

    assert safe.status == "NOT_FOUND"
    assert "secret-token" not in str(safe)
    assert "user-text" not in str(safe)


def test_firestore_errors_ignore_untrusted_status_text():
    error = RuntimeError("secret detail")
    error.code = "SECRET_TOKEN"  # type: ignore[attr-defined]

    safe = convert_firestore_error("read", error)

    assert safe.status is None
    assert str(safe) == "Firestore read failed"
