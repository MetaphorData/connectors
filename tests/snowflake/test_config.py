from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.config import SnowflakeRunConfig


def test_yaml_config(test_root_dir):
    config = SnowflakeRunConfig.from_yaml_file(f"{test_root_dir}/snowflake/config.yml")

    assert config == SnowflakeRunConfig(
        account="account",
        user="user",
        password="password",
        default_database="database",
        filter=DatasetFilter(
            includes={
                "db1": {
                    "schema1": None,
                },
                "db2": {
                    "schema2": set(
                        [
                            "table1",
                            "table2",
                        ]
                    )
                },
            },
            excludes={"db3": None},
        ),
        output=None,
    )
