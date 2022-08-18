from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.lineage.config import SnowflakeLineageRunConfig


def test_yaml_config(test_root_dir):
    config = SnowflakeLineageRunConfig.from_yaml_file(
        f"{test_root_dir}/snowflake/lineage/config.yml"
    )

    assert config == SnowflakeLineageRunConfig(
        account="account",
        user="user",
        password="password",
        enable_view_lineage=True,
        enable_lineage_from_history=True,
        filter=DatasetFilter(),
        lookback_days=7,
        output=OutputConfig(),
    )
