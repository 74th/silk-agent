import pytest
from pydantic import ValidationError

from silk_agent.sandbox.protocol import CommandStartRequest, CommandStartResponse


def test_command_start_requires_fenced_identity_and_has_strict_integers():
    request = CommandStartRequest(
        request_id="request-1",
        conversation_key="slack:T:C:123",
        owner_epoch=2,
        conversation_generation=3,
        turn_id="turn-4",
        origin_seq=5,
        command="echo ready",
    )
    assert request.owner_epoch == 2
    assert request.timeout_seconds == 0
    with pytest.raises(ValidationError):
        CommandStartRequest(
            request_id="request-1",
            conversation_key="slack:T:C:123",
            owner_epoch=True,
            conversation_generation=3,
            turn_id="turn-4",
            origin_seq=5,
            command="echo ready",
        )


def test_command_response_contains_request_id_state_and_sandbox_generation():
    response = CommandStartResponse(
        request_id="request-1",
        command_id="request-1",
        state="accepted",
        sandbox_generation=3,
    )
    assert response.model_dump(mode="json")["sandbox_generation"] == 3
