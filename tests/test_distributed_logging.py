import json
import logging

from silk_agent.distributed.logging import RoleLogFormatter


def test_role_logs_preserve_request_metadata_without_body_token_or_exception_repr():
    record = logging.LogRecord(
        "silk_agent.sandbox.service",
        logging.ERROR,
        __file__,
        1,
        "sandbox service request failed",
        (),
        (ValueError, ValueError("secret-in-exception"), None),
    )
    record.request_id = "request-1"
    record.status = 500
    record.error_type = "ValueError"
    record.token = "secret-token"
    record.body = {"text": "private-input"}
    formatted = RoleLogFormatter().format(record)
    result = json.loads(formatted)
    assert result["request_id"] == "request-1"
    assert result["status"] == 500
    assert result["error_type"] == "ValueError"
    assert "secret" not in formatted and "private-input" not in formatted
    assert "token" not in result and "body" not in result
