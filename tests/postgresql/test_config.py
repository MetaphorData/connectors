from metaphor.common.aws import AwsCredentials
from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.postgresql.config import PostgreSQLQueryLogConfig
from metaphor.postgresql.extractor import PostgreSQLRunConfig


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
        output=OutputConfig(),
        query_log=PostgreSQLQueryLogConfig(
            lookback_days=1,
            excluded_usernames={"foo", "bar"},
            aws=AwsCredentials(
                access_key_id="access_id",
                secret_access_key="secret",
                assume_role_arn="arn:aws:iam::12312837197:role/MetaphorRole",
                region_name="us-west-2",
            ),
            logs_group="/aws/rds/instance/postgres/postgresql",
        ),
    )
