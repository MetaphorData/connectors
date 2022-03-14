from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.lineage.config import SnowflakeLineageRunConfig


def test_yaml_config(test_root_dir):
    config = SnowflakeLineageRunConfig.from_yaml_file(
        f"{test_root_dir}/snowflake/usage/config.yml"
    )

    assert config == SnowflakeLineageRunConfig(
        account="account",
        user="user",
        password="password",
        filter=DatasetFilter(),
        lookback_days=30,
        output=OutputConfig(),
    )
