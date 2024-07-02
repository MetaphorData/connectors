import pytest
from pydantic import ValidationError

from metaphor.common.base_config import OutputConfig
from metaphor.common.git import GitRepoConfig
from metaphor.looker.config import LookerConnectionConfig, LookerRunConfig
from metaphor.models.metadata_change_event import DataPlatform


def test_yaml_config(test_root_dir):
    config = LookerRunConfig.from_yaml_file(f"{test_root_dir}/looker/config.yml")

    assert config == LookerRunConfig(
        base_url="base_url",
        client_id="client_id",
        client_secret="client_secret",
        lookml_dir="lookml_dir",
        connections={
            "conn1": LookerConnectionConfig(
                database="db1",
                default_schema="schema1",
                platform=DataPlatform.SNOWFLAKE,
                account="account1",
            ),
            "conn2": LookerConnectionConfig(
                database="db2",
                default_schema="schema2",
                platform=DataPlatform.BIGQUERY,
            ),
        },
        project_source_url="http://foo.bar",
        verify_ssl=True,
        timeout=1,
        include_personal_folders=False,
        output=OutputConfig(),
    )


def test_yaml_config_with_git(test_root_dir):
    config = LookerRunConfig.from_yaml_file(f"{test_root_dir}/looker/config2.yml")

    assert config == LookerRunConfig(
        base_url="base_url",
        client_id="client_id",
        client_secret="client_secret",
        connections={
            "conn1": LookerConnectionConfig(
                database="db1",
                account="account1",
                default_schema="schema1",
                platform="SNOWFLAKE",
            )
        },
        lookml_git_repo=GitRepoConfig(
            git_url="https://github.com/foo/looker.git",
            username="foo",
            access_token="bar",
        ),
        include_personal_folders=True,
        output=OutputConfig(),
    )


def test_yaml_config_missing_lookml(test_root_dir):
    with pytest.raises(ValidationError):
        LookerRunConfig.from_yaml_file(
            f"{test_root_dir}/looker/config_missing_lookml.yml"
        )
