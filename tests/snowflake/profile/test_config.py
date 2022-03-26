from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.profile.config import (
    ColumnStatistics,
    SnowflakeProfileRunConfig,
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
        column_statistics=ColumnStatistics(
            null_count=True,
            unique_count=True,
            min_value=True,
            max_value=True,
            avg_value=True,
            std_dev=True,
        ),
        filter=DatasetFilter(includes={"foo": None, "bar": None}, excludes=None),
        output=OutputConfig(),
    )
