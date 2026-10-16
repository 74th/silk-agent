import asyncio
import subprocess
import sys
import time

sys.path.insert(0, __import__("os").path.dirname(__file__))
from acceptance_common import *


async def main():
    e = Env()
    k = f"acceptance:{uuid.uuid4().hex[:8]}"
    await e.send(k, "Remember the word PINEAPPLE. Reply with just ok.")

    async def fin():
        o = await e.outputs(k)
        return o if "TurnFinished" in kinds(o) else None

    await e.wait(fin, 300)
    a = await e.own.get(k)
    print("before rollout: owner", a.worker_id[-5:])
    t = time.time()
    subprocess.run(
        [
            "kubectl",
            "-n",
            "session-sandbox-system",
            "rollout",
            "restart",
            "deploy/silk-agent-worker",
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            "kubectl",
            "-n",
            "session-sandbox-system",
            "rollout",
            "status",
            "deploy/silk-agent-worker",
            "--timeout=300s",
        ],
        check=True,
        capture_output=True,
    )
    print("rollout done in %.0fs" % (time.time() - t))
    await e.send(k, "What word did I ask you to remember? Answer with the word only.")

    async def second():
        o = await e.outputs(k)
        return o if kinds(o).count("TurnFinished") >= 2 else None

    t = time.time()
    o = await e.wait(second, 300)
    n = await e.own.get(k)
    print("follow-up answered in %.0fs by" % (time.time() - t), n.worker_id[-5:])
    e.show(o, len(o) - 3)


asyncio.run(main())
