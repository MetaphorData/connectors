from metaphor.snowflake.filter import SnowflakeFilter
from metaphor.snowflake.usage.config import SnowflakeUsageRunConfig


def test_json_config(test_root_dir):
    config = SnowflakeUsageRunConfig.from_json_file(
        f"{test_root_dir}/snowflake/usage/config.json"
    )

    assert config == SnowflakeUsageRunConfig(
        account="account",
        user="user",
        password="password",
        filter=SnowflakeFilter(
            includes={"DEMO_DB": None},
            excludes={"db1": {"schema1": set()}},
        ),
        lookback_days=365,
        excluded_usernames={"METAPHOR"},
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = SnowflakeUsageRunConfig.from_yaml_file(
        f"{test_root_dir}/snowflake/usage/config.yml"
    )

    assert config == SnowflakeUsageRunConfig(
        account="account",
        user="user",
        password="password",
        filter=SnowflakeFilter(),
        lookback_days=30,
        excluded_usernames=set(),
        output=None,
    )
