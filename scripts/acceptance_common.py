import asyncio
from datetime import UTC, datetime

from silk_agent.config import DistributedExecutionConfig
from silk_agent.core.clock import SystemClock
from silk_agent.distributed.control_runtime import ControlAllocatorLoop
from silk_agent.stores.firestore.allocator import FirestoreAllocator
from silk_agent.stores.firestore.client import create_firestore_client
from silk_agent.stores.firestore.conversations import FirestoreConversationStore
from silk_agent.stores.firestore.inbox import FirestoreInboxStore
from silk_agent.stores.firestore.ownership import FirestoreOwnershipStore
from silk_agent.stores.firestore.workers import FirestoreWorkerDirectory

P = "silk-agent"
SENDER = {"email": "acceptance@example.com", "is_guest": False, "is_external": False, "team": "T"}


class Env:
    def __init__(s):
        s.c = create_firestore_client(
            DistributedExecutionConfig(
                firestore_project="nnyn-dev", firestore_database="silk-agent"
            )
        )
        s.inbox = FirestoreInboxStore(s.c, collection_prefix=P)
        s.conv = FirestoreConversationStore(s.c, collection_prefix=P)
        s.d = FirestoreWorkerDirectory(s.c, collection_prefix=P)
        s.own = FirestoreOwnershipStore(s.c, collection_prefix=P)
        s.loop = ControlAllocatorLoop(
            s.conv,
            s.d,
            FirestoreAllocator(s.c, collection_prefix=P),
            SystemClock(),
            allocator_id="acceptance-local",
        )
        s.seq = {}

    async def send(s, k, text, gen=1):
        n = s.seq[k] = s.seq.get(k, 0) + 1
        await s.inbox.accept_input(
            conversation_key=k,
            input_id=f"in-{n}",
            conversation_generation=gen,
            kind="message",
            payload={"text": text, "user": "UACC", "team": "T", "channel": "C", "sender": SENDER},
            created_at=datetime.now(UTC),
        )

    async def control(s, k, kind, gen=1):
        n = s.seq[k] = s.seq.get(k, 0) + 1
        c = await s.inbox.accept_control(
            conversation_key=k,
            input_id=f"ctl-{n}",
            conversation_generation=gen,
            kind=kind,
            sender_id="UACC",
            payload={"text": "stop", "user": "UACC", "sender": SENDER},
            created_at=datetime.now(UTC),
        )
        assert await s.inbox.resolve_control_authorization(
            conversation_key=k, input_id=c.input_id, authorized=True, now=datetime.now(UTC)
        )

    async def outputs(s, k):
        out = []
        async for x in s.c.collection(f"{P}_outbox").where("conversation_key", "==", k).stream():
            out.append(x.to_dict())
        return sorted(out, key=lambda r: r["sequence"])

    async def wait(s, pred, secs=240):
        for _ in range(secs // 2):
            await s.loop.run_once()
            r = await pred()
            if r:
                return r
            await asyncio.sleep(2)
        raise TimeoutError

    def show(s, o, start=0):
        for r in o[start:]:
            ev = r["payload"]["event"]
            print(
                "  ",
                r["sequence"],
                r["payload"]["kind"],
                str(ev.get("text") or ev.get("summary") or ev.get("reason") or "")[:120],
            )


def kinds(o):
    return [r["payload"]["kind"] for r in o]
