"""Mark the outputs of acceptance/load-test conversations delivered so Control never posts them
to Slack, and cancel their unfinished inputs.

    uv run python scripts/acceptance_cleanup.py
"""

import asyncio
import sys

sys.path.insert(0, __import__("os").path.dirname(__file__))
from acceptance_common import *  # noqa: E402,F403

PREFIXES = ("acceptance:", "loadtest:")


async def main() -> None:
    e = Env()
    outputs = inputs = 0
    async for doc in e.c.collection(f"{P}_outbox").stream():
        data = doc.to_dict()
        if (
            data.get("conversation_key", "").startswith(PREFIXES)
            and data.get("status") != "delivered"
        ):
            await doc.reference.update({"status": "delivered", "suppressed_reason": "test"})
            outputs += 1
    async for doc in e.c.collection(f"{P}_conversations").stream():
        if not doc.to_dict().get("conversation_key", "").startswith(PREFIXES):
            continue
        async for item in doc.reference.collection("inputs").stream():
            if item.to_dict().get("delivery_state") in ("pending", "queued"):
                await item.reference.update({"delivery_state": "cancelled"})
                inputs += 1
    print(f"outputs suppressed {outputs}, inputs cancelled {inputs}")


asyncio.run(main())
