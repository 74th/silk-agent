"""Slack-free end-to-end check of the bigquery-analysis use case on the real cluster.

An input goes to a conversation of the use case; its Worker runs the agent, which reads the
`google-trends` Skill and runs the script in the Sandbox with the use case's service account, so a
real BigQuery query runs. Expected: the reply names search terms from the rows.

    ACCEPTANCE_PREFIX=bigquery-analysis ACCEPTANCE_TEAM=<allowed_teams of its config> \
        uv run python scripts/acceptance_bigquery.py
"""

import asyncio
import sys

sys.path.insert(0, __import__("os").path.dirname(__file__))
from acceptance_common import *  # noqa: E402,F403


async def main() -> None:
    assert P == "bigquery-analysis", "run it with ACCEPTANCE_PREFIX=bigquery-analysis"  # noqa: F405
    e = Env()
    key = f"acceptance:{uuid.uuid4().hex[:8]}"  # noqa: F405
    await e.send(key, "日本の今日の急上昇検索語を調べて、上位3件を日本語で教えてください。")

    async def finished():
        outputs = await e.outputs(key)
        return outputs if "TurnFinished" in kinds(outputs) else None  # noqa: F405

    outputs = await e.wait(finished, 600)
    tools = [
        r["payload"]["event"].get("summary", "")
        for r in outputs
        if r["payload"]["kind"] == "ToolStarted"
    ]
    for line in tools:
        print("tool:", line[:120])
    e.show(outputs, 0)
    text = " ".join(
        r["payload"]["event"].get("text", "")
        for r in outputs
        if r["payload"]["kind"] == "AssistantText"
    )
    assert any("query.sh" in line for line in tools), "the Skill script was not run"
    assert "rank" in text or "位" in text or "1" in text, text
    print("OK")


asyncio.run(main())
