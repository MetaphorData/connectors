from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import TwoLevelDatasetFilter
from metaphor.mysql.extractor import MySQLRunConfig


def test_yaml_config(test_root_dir):
    config = MySQLRunConfig.from_yaml_file(f"{test_root_dir}/mysql/config.yml")

    assert config == MySQLRunConfig(
        host="host",
        database="database",
        user="user",
        password="password",
        filter=TwoLevelDatasetFilter(
            includes={
                "schema1": None,
                "schema2": set(
                    [
                        "table1",
                        "table2",
                    ]
                ),
            },
            excludes={"schema3": None},
        ),
        port=1234,
        output=OutputConfig(),
    )
