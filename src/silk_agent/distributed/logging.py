"""Structured role logs with an explicit metadata allowlist."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

_FIELDS = frozenset(
    {
        "request_id",
        "method",
        "route",
        "status",
        "duration_ms",
        "error_type",
        "assigned",
        "port",
    }
)


class RoleLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        value: dict[str, str | int | float | bool] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in _FIELDS:
            item = getattr(record, field, None)
            if isinstance(item, (str, int, float, bool)):
                value[field] = item
        # Exception reprs and arbitrary extras can contain request bodies/credentials.
        # Call sites log a safe exception class through error_type when needed.
        return json.dumps(value, ensure_ascii=False)


def configure_role_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(RoleLogFormatter())
    logging.basicConfig(level=level, handlers=[handler])
