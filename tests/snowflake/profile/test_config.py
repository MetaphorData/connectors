from metaphor.snowflake.filter import SnowflakeFilter
from metaphor.snowflake.profile.config import SnowflakeProfileRunConfig


def test_json_config(test_root_dir):
    config = SnowflakeProfileRunConfig.from_json_file(
        f"{test_root_dir}/snowflake/profile/config.json"
    )

    assert config == SnowflakeProfileRunConfig(
        account="account",
        user="user",
        password="password",
        default_database="database",
        filter=SnowflakeFilter(),
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = SnowflakeProfileRunConfig.from_yaml_file(
        f"{test_root_dir}/snowflake/profile/config.yml"
    )

    assert config == SnowflakeProfileRunConfig(
        account="account",
        user="user",
        password="password",
        default_database="database",
        filter=SnowflakeFilter(includes={"foo": None, "bar": None}, excludes=None),
        output=None,
    )
