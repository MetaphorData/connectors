import pytest
from pydantic import ValidationError

from metaphor.bigquery.config import BigQueryCredentials, BigQueryQueryLogConfig
from metaphor.bigquery.extractor import BigQueryRunConfig
from metaphor.common.base_config import OutputConfig


def test_yaml_config_with_key_path(test_root_dir):
    config = BigQueryRunConfig.from_yaml_file(f"{test_root_dir}/bigquery/config.yml")

    assert config == BigQueryRunConfig(
        key_path="key_path",
        project_ids=["project_id"],
        job_project_id="job_project_id",
        query_log=BigQueryQueryLogConfig(
            lookback_days=7, excluded_usernames={"ex1", "ex2"}
        ),
        output=OutputConfig(),
    )


def test_yaml_config_with_credentials(test_root_dir):
    config = BigQueryRunConfig.from_yaml_file(f"{test_root_dir}/bigquery/config2.yml")

    assert config == BigQueryRunConfig(
        output=OutputConfig(api=None, file=None),
        key_path=None,
        credentials=BigQueryCredentials(
            project_id="metaphor",
            private_key_id="1234",
            private_key="-----BEGIN PRIVATE KEY-----\nabcd\n-----END PRIVATE KEY-----\n",
            client_email="foo@gserviceaccount.com",
            client_id="9876",
            type="service_account",
            auth_uri="https://accounts.google.com/o/oauth2/auth",
            token_uri="https://oauth2.googleapis.com/token",
        ),
        project_ids=["project1", "project2"],
    )


def test_yaml_config_with_missing_config(test_root_dir):
    with pytest.raises(ValidationError):
        BigQueryRunConfig.from_yaml_file(
            f"{test_root_dir}/bigquery/config_missing_credentials.yml"
        )
