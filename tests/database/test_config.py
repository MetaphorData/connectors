from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import TwoLevelDatasetFilter
from metaphor.database.extractor import GenericDatabaseRunConfig


def test_yaml_config(test_root_dir):
    config = GenericDatabaseRunConfig.from_yaml_file(
        f"{test_root_dir}/database/config.yml"
    )

    assert config == GenericDatabaseRunConfig(
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
        port=3306,
        output=OutputConfig(),
    )
