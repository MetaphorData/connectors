from metaphor.common.base_config import OutputConfig
from metaphor.models.metadata_change_event import DataPlatform
from metaphor.monte_carlo.config import MonteCarloRunConfig


def test_yaml_config(test_root_dir):
    config = MonteCarloRunConfig.from_yaml_file(
        f"{test_root_dir}/monte_carlo/config.yml"
    )

    assert config == MonteCarloRunConfig(
        api_key_id="key_id",
        api_key_secret="key_secret",
        data_platform=DataPlatform.SNOWFLAKE,
        output=OutputConfig(),
    )
