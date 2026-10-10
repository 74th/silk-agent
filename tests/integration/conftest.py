import os

import pytest

from silk_agent.stores.agent_platform import AgentPlatformSessions

PROJECT = "nnyn-dev"
LOCATION = "us-central1"


@pytest.fixture(scope="session")
def agent_runtime() -> str:
    """Agent Runtime instance that holds the test sessions (set SILK_AGENT_TEST_RUNTIME)."""
    value = os.environ.get("SILK_AGENT_TEST_RUNTIME", "")
    if not value:
        pytest.skip("SILK_AGENT_TEST_RUNTIME is not set")
    return value


@pytest.fixture
def sessions_api(agent_runtime: str) -> AgentPlatformSessions:
    # Short TTL so the test sessions clean themselves up.
    return AgentPlatformSessions(
        agent_runtime, project=PROJECT, location=LOCATION, ttl_seconds=24 * 3600
    )
