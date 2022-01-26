from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.usage.config import SnowflakeUsageRunConfig


def test_yaml_config(test_root_dir):
    config = SnowflakeUsageRunConfig.from_yaml_file(
        f"{test_root_dir}/snowflake/usage/config.yml"
    )

    assert config == SnowflakeUsageRunConfig(
        account="account",
        user="user",
        password="password",
        filter=DatasetFilter(),
        lookback_days=30,
        excluded_usernames=set(),
        output=None,
    )
