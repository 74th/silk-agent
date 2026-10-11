from __future__ import annotations

import asyncio
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from examples.bot_agent_turn import run_agent_turn
from gke_session_sandbox import CommandResult, EventType, SessionEvent, SessionKey


class ConversationStore:
    def __init__(self):
        self.event_ids = set()
        self.messages = []

    async def append_once(self, event_id, message):
        if event_id not in self.event_ids:
            self.event_ids.add(event_id)
            self.messages.append(message)


class Commands:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def run(self, argv, **kwargs):
        self.argv = argv
        self.kwargs = kwargs
        return CommandResult(
            command_id="turn-1", exit_code=0, stdout=b"turn complete", stderr=b"",
            stop_reason=None, output_complete=True,
        )


class Handle:
    def __init__(self, events):
        self.pending_context_events = events
        self.commands = Commands()


class Manager:
    def __init__(self, events, conversation_store):
        self.handle = Handle(events)
        self.conversation_store = conversation_store
        self.acked = []

    async def ensure(self, _key):
        return self.handle

    async def ack_context_event(self, _key, event_id):
        assert event_id in self.conversation_store.event_ids
        self.acked.append(event_id)
        return True


@pytest.mark.asyncio
async def test_bot_adapter_persists_events_before_ack_and_runs_one_command():
    event = SessionEvent(
        event_id="session:1:lost", generation=1,
        event_type=EventType.WORKSPACE_LOST,
        occurred_at="2030-01-01T00:00:00Z", reason="external-pvc-loss",
        context_message="The previous workspace was lost.",
    )
    store = ConversationStore()
    manager = Manager((event,), store)
    key = SessionKey(tenant_id="tenant", session_id="channel-thread")

    result = await run_agent_turn(
        manager, store, key, "hello", agent_argv=("python3", "/opt/agent/one_turn_agent.py")
    )

    assert manager.acked == [event.event_id]
    assert store.messages == [event.context_message]
    assert result.exit_code == 0
    assert manager.handle.commands.argv == [
        "python3", "/opt/agent/one_turn_agent.py", "hello"
    ]
    assert manager.handle.commands.kwargs == {"check": True, "timeout_seconds": 600}


def test_one_turn_agent_fixture_exits_after_simulated_llm_wait():
    fixture = Path(__file__).parents[2] / "examples" / "fixtures" / "one_turn_agent.py"
    result = asyncio.run(_run_fixture(fixture))
    assert result.returncode == 0
    assert result.stdout.strip() == "turn complete: waiting prompt"


async def _run_fixture(fixture: Path):
    process = await asyncio.create_subprocess_exec(
        sys.executable, str(fixture), "waiting prompt", "--wait-seconds", "0.1",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=2)
    return type("ProcessResult", (), {
        "returncode": process.returncode,
        "stdout": stdout.decode(),
        "stderr": stderr.decode(),
    })()
