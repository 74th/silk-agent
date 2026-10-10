"""SessionStore conformance against the real Agent Platform Sessions. Run with `-m integration`."""

import json
import time
import uuid

import pytest
from claude_agent_sdk.testing import run_session_store_conformance

from silk_agent.stores.agent_platform import (
    AgentPlatformSessions,
    AgentPlatformSessionStore,
)

pytestmark = pytest.mark.integration


async def test_session_store_conformance(sessions_api: AgentPlatformSessions):
    store = AgentPlatformSessionStore(sessions_api)

    class Isolated:
        """The conformance suite reuses fixed session ids; isolate every contract run by
        prefixing the SDK session id so runs never see each other's events."""

        def __init__(self) -> None:
            self.prefix = uuid.uuid4().hex[:12]

        def _k(self, key):
            return {**key, "session_id": f"{self.prefix}-{key['session_id']}"}

        async def append(self, key, entries):
            await store.append(self._k(key), entries)

        async def load(self, key):
            return await store.load(self._k(key))

    await run_session_store_conformance(Isolated)


async def test_event_size_limit_probe(sessions_api: AgentPlatformSessions):
    """Find how large one entry can be (recorded in design.md)."""
    store = AgentPlatformSessionStore(sessions_api)
    key = {"project_key": "probe", "session_id": uuid.uuid4().hex[:16]}
    results: dict[int, str] = {}
    for kb in (64, 256, 512, 1024, 2048, 4096):
        entry = {"type": "user", "uuid": f"u{kb}", "payload": "x" * (kb * 1024)}
        started = time.time()
        try:
            await store.append(key, [entry])
            loaded = await store.load(key)
            ok = bool(loaded) and any(e.get("uuid") == f"u{kb}" for e in loaded)
            results[kb] = f"ok={ok} {time.time() - started:.1f}s"
        except Exception as e:
            results[kb] = f"FAILED {type(e).__name__}: {str(e)[:120]}"
            break
    print("EVENT_SIZE_PROBE", json.dumps(results))
    assert results[64].startswith("ok=True")
