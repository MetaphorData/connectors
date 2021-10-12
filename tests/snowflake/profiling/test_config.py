from metaphor.snowflake.profiling.extractor import SnowflakeProfilingRunConfig


def test_json_config(test_root_dir):
    config = SnowflakeProfilingRunConfig.from_json_file(
        f"{test_root_dir}/snowflake/config.json"
    )

    assert config == SnowflakeProfilingRunConfig(
        account="account",
        user="user",
        password="password",
        default_database="database",
        target_databases=None,
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = SnowflakeProfilingRunConfig.from_yaml_file(
        f"{test_root_dir}/snowflake/config.yml"
    )

    assert config == SnowflakeProfilingRunConfig(
        account="account",
        user="user",
        password="password",
        default_database="database",
        target_databases=["foo", "bar"],
        output=None,
    )
