"""Mark the outputs of acceptance/load-test conversations delivered so Control never posts them
to Slack, and cancel their unfinished inputs.

    uv run python scripts/acceptance_cleanup.py
"""

import asyncio
import sys
from datetime import UTC, datetime, timedelta

sys.path.insert(0, __import__("os").path.dirname(__file__))
from acceptance_common import *  # noqa: E402,F403

PREFIXES = ("acceptance:", "loadtest:")


async def main() -> None:
    e = Env()
    outputs = inputs = expiring = 0
    expire_at = datetime.now(UTC) + timedelta(days=1)  # test records are not kept
    async for doc in e.c.collection(f"{P}_outbox").stream():
        data = doc.to_dict()
        if not data.get("conversation_key", "").startswith(PREFIXES):
            continue
        if data.get("status") != "delivered":
            await doc.reference.update(
                {"status": "delivered", "suppressed_reason": "test", "expire_at": expire_at}
            )
            outputs += 1
        elif data.get("expire_at") is None:  # delivered earlier (e.g. by the load test)
            await doc.reference.update({"expire_at": expire_at})
            expiring += 1
    async for doc in e.c.collection(f"{P}_conversations").stream():
        if not doc.to_dict().get("conversation_key", "").startswith(PREFIXES):
            continue
        async for item in doc.reference.collection("inputs").stream():
            if item.to_dict().get("delivery_state") in ("pending", "queued"):
                await item.reference.update({"delivery_state": "cancelled", "expire_at": expire_at})
                inputs += 1
    print(f"outputs suppressed {outputs}, given an expiry {expiring}, inputs cancelled {inputs}")


asyncio.run(main())
