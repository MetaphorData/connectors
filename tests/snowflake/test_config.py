from metaphor.snowflake.config import SnowflakeRunConfig
from metaphor.snowflake.filter import SnowflakeFilter


def test_json_config(test_root_dir):
    config = SnowflakeRunConfig.from_json_file(f"{test_root_dir}/snowflake/config.json")

    assert config == SnowflakeRunConfig(
        account="account",
        user="user",
        password="password",
        default_database="database",
        filter=SnowflakeFilter(includes=None, excludes=None),
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = SnowflakeRunConfig.from_yaml_file(f"{test_root_dir}/snowflake/config.yml")

    assert config == SnowflakeRunConfig(
        account="account",
        user="user",
        password="password",
        default_database="database",
        filter=SnowflakeFilter(
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
