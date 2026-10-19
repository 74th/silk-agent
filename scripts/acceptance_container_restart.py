"""Slack-free check that a Worker whose container restarts (same Pod, same UID) is recovered
automatically.

A conversation is started on a Worker, that Worker's container is killed (the Pod stays, only
the process ends, as with an OOM kill) and a follow-up arrives. Expected: the conversation
recovers without a manual `recovery confirm-stopped`, and the agent still knows what it was told. (The evidence, `auto:incarnation-replaced:<pod>`, is
not kept: the next owner's record replaces it. The Pod is still there, so it cannot be the
Pod-absence evidence.)

    uv run python scripts/acceptance_container_restart.py
"""

import asyncio
import subprocess
import sys
import time

sys.path.insert(0, __import__("os").path.dirname(__file__))
from acceptance_common import *  # noqa: E402,F403

NS = "session-sandbox-system"


def kubectl(*args: str) -> str:
    result = subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True)
    return result.stdout.strip()


# From inside a container nobody can SIGKILL its PID 1, so a crash (an OOM kill) is made from the
# node's PID namespace: a short-lived privileged Pod on the Worker's node finds the Worker's
# process by its POD_NAME and kills it with SIGKILL. The kubelet then restarts the container.
KILL_CODE = """
import os, signal, sys
target = ("POD_NAME=" + sys.argv[1]).encode()
for entry in os.listdir("/proc"):
    if not entry.isdigit():
        continue
    try:
        env = open(f"/proc/{entry}/environ", "rb").read().split(b"\\0")
        cmd = open(f"/proc/{entry}/cmdline", "rb").read()
    except OSError:
        continue
    if target in env and b"worker" in cmd and b"run" in cmd:
        os.kill(int(entry), signal.SIGKILL)
        print("killed", entry)
"""


def kill_container(pod: str) -> None:
    import json

    node = kubectl("get", "pod", pod, "-o", "jsonpath={.spec.nodeName}")
    image = kubectl("get", "pod", pod, "-o", "jsonpath={.spec.containers[0].image}")
    name = f"kill-{uuid.uuid4().hex[:6]}"  # noqa: F405
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {
            "nodeName": node,
            "hostPID": True,
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "kill",
                    "image": image,
                    "command": ["python", "-c", KILL_CODE, pod],
                    "securityContext": {"privileged": True, "runAsUser": 0},
                }
            ],
        },
    }
    subprocess.run(
        ["kubectl", "apply", "-f", "-"], input=json.dumps(manifest), text=True, check=True
    )
    for _ in range(60):
        phase = subprocess.run(
            ["kubectl", "-n", "default", "get", "pod", name, "-o", "jsonpath={.status.phase}"],
            capture_output=True,
            text=True,
        ).stdout
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(2)
    logs = subprocess.run(
        ["kubectl", "-n", "default", "logs", name], capture_output=True, text=True
    ).stdout
    print("kill pod:", phase, logs.strip())
    subprocess.run(["kubectl", "-n", "default", "delete", "pod", name, "--wait=false"])
    assert "killed" in logs, "the Worker's process was not found on its node"


async def main() -> None:
    e = Env()
    key = f"acceptance:{uuid.uuid4().hex[:8]}"  # noqa: F405
    await e.send(key, "Remember the word MANGO. Reply with just ok.")

    async def first():
        o = await e.outputs(key)
        return o if "TurnFinished" in kinds(o) else None  # noqa: F405

    await e.wait(first, 300)
    owner = await e.own.get(key)
    pod = owner.worker_id
    before = kubectl("get", "pod", pod, "-o", "jsonpath={.metadata.uid} {..restartCount}")
    print("owner pod:", pod, "(uid, restarts:", before + ")")

    # SIGKILL the container's process: the kubelet restarts the container inside the same Pod.
    kill_container(pod)
    for _ in range(60):
        after = kubectl("get", "pod", pod, "-o", "jsonpath={.metadata.uid} {..restartCount}")
        if after != before:
            break
        await asyncio.sleep(2)
    print("after the kill (uid, restarts):", after)
    assert after.split()[0] == before.split()[0], "the Pod must be the same (same UID)"
    assert after != before, "the container did not restart"

    # A made-up user (the Worker needs a non-empty one): the Gateway would look it up in Slack,
    # give up within seconds and block the input, long before the recovery (minutes) can start.
    # So the lookup is marked resolved on the stored record, and `sender` is stored with it.
    follow_up = f"in-restart-{uuid.uuid4().hex[:6]}"  # noqa: F405
    await e.inbox.accept_input(
        conversation_key=key,
        input_id=follow_up,
        conversation_generation=1,
        kind="message",
        payload={
            "text": "What word did I ask you to remember? Answer with the word only.",
            "user": "UACC",
            "team": "T",
            "channel": "C",
            "sender": SENDER,  # noqa: F405
        },
        created_at=datetime.now(UTC),  # noqa: F405
    )
    conversation = e.c.collection(f"{P}_conversations").document(e.conv._conversation_id(key))
    async for item in conversation.collection("inputs").stream():
        if item.to_dict().get("input_id") == follow_up:
            await item.reference.update({"sender_lookup_state": "resolved"})
    started = time.time()

    async def second():
        o = await e.outputs(key)
        return o if kinds(o).count("TurnFinished") >= 2 else None  # noqa: F405

    seen: list[str] = []

    async def timeline():
        """Print each change of the ownership, with the seconds since the follow-up."""
        record = await e.own.get(key)
        new_owner = await e.own.get(key)

    print("new owner pod:", new_owner.worker_id if new_owner else None)


asyncio.run(main())
