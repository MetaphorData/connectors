from metaphor.common.filter import DatasetFilter
from metaphor.postgresql.extractor import PostgreSQLRunConfig


def test_json_config(test_root_dir):
    config = PostgreSQLRunConfig.from_json_file(
        f"{test_root_dir}/postgresql/config.json"
    )

    assert config == PostgreSQLRunConfig(
        host="host",
        database="database",
        user="user",
        password="password",
        filter=DatasetFilter(includes=None, excludes=None),
        port=5432,
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = PostgreSQLRunConfig.from_yaml_file(
        f"{test_root_dir}/postgresql/config.yml"
    )

    assert config == PostgreSQLRunConfig(
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
        output=None,
    )
