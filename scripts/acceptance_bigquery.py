"""Slack-free end-to-end check of the bigquery-analysis use case on the real cluster.

An input goes to a conversation of the use case; its Worker runs the agent, which reads the
`google-trends` Skill and runs the script in the Sandbox with the use case's service account, so a
real BigQuery query runs. Expected: the reply names the top three search terms that the same SQL
returns when it is run here, directly (so a failed query, or an agent that answers "the daily limit
is reached", does not pass).

    ACCEPTANCE_PREFIX=bigquery-analysis ACCEPTANCE_TEAM=<allowed_teams of its config> \
        uv run python scripts/acceptance_bigquery.py
"""

import asyncio
import json
import pathlib
import subprocess
import sys

sys.path.insert(0, __import__("os").path.dirname(__file__))
from acceptance_common import *  # noqa: E402,F403

SQL = pathlib.Path(__file__).parent.parent / (
    "usecases/bigquery-analysis/skills/google-trends/scripts/query.sql"
)


def expected_top_terms(count: int = 3) -> list[str]:
    """The first rows of the Skill's own SQL, run directly with the operator's credentials."""
    out = subprocess.run(
        ["bq", "query", "--nouse_legacy_sql", "--format=json", "--project_id=nnyn-dev"],
        input=SQL.read_text(encoding="utf-8"),
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    rows = json.loads(out[out.index("[") :])
    return [row["term"] for row in rows[:count]]


async def main() -> None:
    assert P == "bigquery-analysis", "run it with ACCEPTANCE_PREFIX=bigquery-analysis"  # noqa: F405
    e = Env()
    key = f"acceptance:{uuid.uuid4().hex[:8]}"  # noqa: F405
    await e.send(key, "日本の今日の急上昇検索語を調べて、上位3件を日本語で教えてください。")

    async def finished():
        outputs = await e.outputs(key)
        return outputs if "TurnFinished" in kinds(outputs) else None  # noqa: F405

    outputs = await e.wait(finished, 600)
    e.show(outputs, 0)
    tools = [
        r["payload"]["event"].get("summary", "")
        for r in outputs
        if r["payload"]["kind"] == "ToolStarted"
    ]
    text = " ".join(
        r["payload"]["event"].get("text", "")
        for r in outputs
        if r["payload"]["kind"] == "AssistantText"
    )
    assert any("query.sh" in line for line in tools), "the Skill script was not run"
    expected = expected_top_terms()
    print("expected (the same SQL, run directly):", expected)
    missing = [term for term in expected if term not in text]
    assert not missing, f"the reply lacks terms the query returns: {missing}\n{text}"
    print("OK")


asyncio.run(main())
