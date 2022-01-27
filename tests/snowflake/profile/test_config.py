from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.profile.config import SnowflakeProfileRunConfig


def test_yaml_config(test_root_dir):
    config = SnowflakeProfileRunConfig.from_yaml_file(
        f"{test_root_dir}/snowflake/profile/config.yml"
    )

    assert config == SnowflakeProfileRunConfig(
        account="account",
        user="user",
        password="password",
        default_database="database",
        filter=DatasetFilter(includes={"foo": None, "bar": None}, excludes=None),
        output=None,
    )
