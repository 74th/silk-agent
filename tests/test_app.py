import pytest

from silk_agent.app import build_services
from silk_agent.config import Config, ConfigError
from silk_agent.testing.fakes import ScriptedChannel


@pytest.mark.asyncio
async def test_in_process_factory_rejects_distributed_configuration(tmp_path):
    config = Config(execution_mode="distributed")
    with pytest.raises(ConfigError, match="distributed role factory"):
        await build_services(config, ScriptedChannel(), work_dir=tmp_path)
