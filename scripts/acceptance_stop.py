"""Slack-free check of stop semantics against the real cluster.

A long turn is stopped, a second request follows right behind the stop, and a third arrives
afterwards. Expected: the first input ends up cancelled (not left 'accepted'), the follow-ups
are processed, and nothing is left unfinished.

    uv run python scripts/acceptance_stop.py
"""

import asyncio
import sys
import time

sys.path.insert(0, __import__("os").path.dirname(__file__))
from acceptance_common import *  # noqa: E402,F403


async def input_states(e, key):
    states = {}
    ref = e.c.collection(f"{P}_conversations").document(e.conv._conversation_id(key))
    async for doc in ref.collection("inputs").stream():
        data = doc.to_dict()
        states[data["input_id"].split("-")[1]] = data["delivery_state"]
    return states


async def main() -> None:
    e = Env()
    key = f"acceptance:{uuid.uuid4().hex[:8]}"  # noqa: F405
    await e.send(
        key, "シェルで `sleep 8` を 4 回、1 回ずつ別々に実行して、終わったら「完了」と言って"
    )

    async def running():
        outputs = await e.outputs(key)
        return outputs if "ToolStarted" in kinds(outputs)[3:] else None  # noqa: F405

    await e.wait(running, 300)
    started = time.time()
    await e.control(key, "stop_candidate")
    await e.send(key, "ありがとうございます。ok とだけ答えてください。")  # right behind the stop

    async def settled():
        states = await input_states(e, key)
        outputs = await e.outputs(key)
        done = "TurnFinished" in kinds(outputs)[kinds(outputs).index("TurnInterrupted") :]  # noqa: F405
        return states if done and "1" in states else None

    await e.wait(settled, 300)
    states = await input_states(e, key)
    print(f"after the stop ({time.time() - started:.0f}s):", states)
    unfinished = [
        s for s in states.values() if s in ("pending", "dispatching", "queued", "accepted")
    ]
    print("unfinished inputs left:", unfinished)
    assert not unfinished, "an input was left unfinished"
    # a later request is still served
    await e.send(key, "もう一度、ok とだけ答えてください。")

    async def served():
        outputs = await e.outputs(key)
        return outputs if kinds(outputs).count("TurnFinished") >= 2 else None  # noqa: F405

    await e.wait(served, 300)
    print("later request served; final states:", await input_states(e, key))


asyncio.run(main())
