from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.postgresql.config import QueryLogConfig
from metaphor.redshift.config import RedshiftRunConfig


def test_yaml_config(test_root_dir):
    config = RedshiftRunConfig.from_yaml_file(f"{test_root_dir}/redshift/config.yml")

    assert config == RedshiftRunConfig(
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
        query_log=QueryLogConfig(
            lookback_days=1,
            excluded_usernames={"metaphor"},
        ),
        output=OutputConfig(),
    )
