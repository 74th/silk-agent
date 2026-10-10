import pytest

from silk_agent.testing.fakes import FakeClock, ScriptedChannel


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def channel() -> ScriptedChannel:
    return ScriptedChannel()
