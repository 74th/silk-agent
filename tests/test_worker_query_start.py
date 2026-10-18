"""The Worker persists the session/input/attempt association before the SDK gets the prompt."""

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from silk_agent.config import Config
from silk_agent.core.distributed_models import InputDeliveryState, OwnershipToken
from silk_agent.distributed.worker_role import WorkerActorFactory
from silk_agent.testing.fakes import InMemoryRuntime
from tests.fakes.distributed_inbox import FakeInboxStore
from tests.fakes.distributed_store import FakeSharedStore

KEY = "T:C:thread"


def _factory(clock, inbox, captured):
    def builder(_config, _runtime, _clock, **kwargs):
        captured.update(kwargs)
        return lambda _key: None

    shared = FakeSharedStore()
    return WorkerActorFactory(
        config=Config(),
        clock=clock,
        inbox=inbox,
        conversations=SimpleNamespace(allocate_output_sequence=None),
        outbox_writer=shared,
        rate_limits=shared,
        command_monitor_store=shared,
        runtime=InMemoryRuntime(),
        session_store=None,
        skills=None,
        work_root=Path("."),
        monitor_id="w",
        build_engine_factory_fn=builder,
    )


@pytest.mark.asyncio
async def test_engine_persists_the_query_start_through_the_inbox(clock):
    inbox = FakeInboxStore()
    token = OwnershipToken(KEY, "w", "inc", 1, 1, clock.now() + timedelta(hours=1))
    inbox.owners[KEY] = token
    envelope = await inbox.accept_input(
        conversation_key=KEY,
        input_id="A",
        conversation_generation=1,
        kind="message",
        payload={"text": "hi"},
        created_at=clock.now(),
    )
    for state in (
        InputDeliveryState.DISPATCHING,
        InputDeliveryState.QUEUED,
        InputDeliveryState.ACCEPTED,
    ):
        assert await inbox.advance_input(
            conversation_key=KEY,
            input_id="A",
            owner=token,
            expected=inbox.inputs[(KEY, "A")].delivery_state,
            new_state=state,
            **({"dispatch_attempt_id": "att-1"} if state is InputDeliveryState.DISPATCHING else {}),
        )
    assert envelope.input_id == "A"
    captured = {}
    _factory(clock, inbox, captured)(token)
    hook = captured["on_query_start"]

    await hook(KEY, "sess-1", "A", "att-1")
    assert inbox.inputs[(KEY, "A")].payload["sdk_session_id"] == "sess-1"

    await hook(KEY, "sess-1", "", "")  # an internal prompt: nothing to associate
    with pytest.raises(RuntimeError):  # not ours any more: the prompt must not be sent
        await hook(KEY, "sess-1", "A", "another-attempt")
