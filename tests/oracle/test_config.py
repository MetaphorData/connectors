from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import TwoLevelDatasetFilter
from metaphor.oracle.config import OracleQueryLogConfig, OracleRunConfig


def test_yaml_config(test_root_dir):
    config = OracleRunConfig.from_yaml_file(f"{test_root_dir}/oracle/config.yml")

    assert config == OracleRunConfig(
        host="host",
        database="database",
        user="user",
        password="password",
        alternative_host="host.foo",
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
        query_logs=OracleQueryLogConfig(
            lookback_days=3,
            excluded_usernames={"foo", "bar"},
        ),
        output=OutputConfig(),
    )
