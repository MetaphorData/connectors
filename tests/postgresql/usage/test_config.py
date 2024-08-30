from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.postgresql.usage.config import PostgreSQLUsageRunConfig


def test_yaml_config(test_root_dir):
    config = PostgreSQLUsageRunConfig.from_yaml_file(
        f"{test_root_dir}/postgresql/usage/config.yml"
    )

    assert config == PostgreSQLUsageRunConfig(
        host="host",
        database="database",
        user="user",
        password="password",
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
        port=1234,
        output=OutputConfig(),
    )
