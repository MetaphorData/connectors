import pytest
from pydantic import ValidationError

from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.auth import SnowflakeKeyPairAuthConfig
from metaphor.snowflake.config import SnowflakeConfig, SnowflakeQueryLogConfig


def test_yaml_config(test_root_dir):
    config = SnowflakeConfig.from_yaml_file(f"{test_root_dir}/snowflake/config.yml")

    assert config == SnowflakeConfig(
        account="account",
        user="user",
        password="password",
        role="role",
        warehouse="warehouse",
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
        query_log=SnowflakeQueryLogConfig(
            lookback_days=7,
            excluded_usernames={"ex1", "ex2"},
            fetch_size=100000,
        ),
        output=OutputConfig(),
    )


def test_yaml_config_with_private_key(test_root_dir):
    config = SnowflakeConfig.from_yaml_file(
        f"{test_root_dir}/snowflake/config_with_key.yml"
    )

    assert config == SnowflakeConfig(
        output=OutputConfig(file=None),
        account="account",
        user="user",
        password=None,
        private_key=SnowflakeKeyPairAuthConfig(
            key_file="key_file", passphrase="passphrase"
        ),
        role="role",
        default_database="database",
        query_tag="MetaphorData",
    )


def test_yaml_config_with_missing_credential(test_root_dir):
    with pytest.raises(ValidationError):
        SnowflakeConfig.from_yaml_file(
            f"{test_root_dir}/snowflake/config_missing_credentials.yml"
        )
