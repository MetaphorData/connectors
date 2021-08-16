from metaphor.snowflake.usage import SnowflakeUsageRunConfig


def test_json_config(test_root_dir):
    config = SnowflakeUsageRunConfig.from_json_file(f"{test_root_dir}/snowflake_usage/config.json")

    assert config == SnowflakeUsageRunConfig(
        account="account",
        user="user",
        password="password",
        lookback_days=365,
        excluded_usernames={'METAPHOR'},
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = SnowflakeUsageRunConfig.from_yaml_file(f"{test_root_dir}/snowflake_usage/config.yml")

    assert config == SnowflakeUsageRunConfig(
        account="account",
        user="user",
        password="password",
        lookback_days=30,
        excluded_usernames=set(),
        output=None,
    )
