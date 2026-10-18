#!/usr/bin/env python3
"""Run the fixed Google Trends query using Application Default Credentials."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any


def _error_kind(error: BaseException) -> str:
    names = {cls.__name__ for cls in type(error).__mro__}
    if names & {"PermissionDenied", "Forbidden", "Unauthorized", "RefreshError"}:
        return "permission"
    if names & {"DeadlineExceeded", "ServiceUnavailable", "RetryError", "ConnectionError"}:
        return "communication"
    if "BadRequest" in names:
        return "query"
    return "authentication_or_api"


def execute_query(client: Any) -> list[dict[str, Any]]:
    sql = Path(__file__).with_name("query.sql").read_text(encoding="utf-8")
    rows = client.query(sql).result()
    return [
        {
            "refresh_date": row["refresh_date"].isoformat(),
            "rank": int(row["rank"]),
            "term": str(row["term"]),
        }
        for row in rows
    ]


def main(client_factory: Callable[..., Any] | None = None) -> int:
    project = os.environ.get("BIGQUERY_JOB_PROJECT", "").strip()
    try:
        if client_factory is None:
            import google.auth
            from google.cloud import bigquery

            credentials, default_project = google.auth.default()
            project = project or default_project or ""
            if not project:
                print(
                    "error: BigQuery project is unavailable from ADC or BIGQUERY_JOB_PROJECT",
                    file=sys.stderr,
                )
                return 2

            def adc_client_factory(**kwargs: Any) -> Any:
                return bigquery.Client(credentials=credentials, **kwargs)

            client_factory = adc_client_factory
        elif not project:
            print("error: BIGQUERY_JOB_PROJECT is not set", file=sys.stderr)
            return 2
        rows = execute_query(client_factory(project=project))
    except Exception as error:
        kind = _error_kind(error)
        print(f"error: BigQuery {kind} failure: {type(error).__name__}", file=sys.stderr)
        return 1

    print(json.dumps({"status": "ok", "rows": rows}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
