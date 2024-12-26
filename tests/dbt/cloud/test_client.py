from typing import Dict
from unittest.mock import patch

from metaphor.dbt.cloud.client import DbtAdminAPIClient, DbtProject
from metaphor.dbt.cloud.parser.common import extract_platform_and_account
from metaphor.models.metadata_change_event import DataPlatform


class Response:
    def __init__(self, status_code: int, json: Dict):
        self.status_code = status_code
        self._json = json

    def json(self) -> Dict:
        return self._json


@patch("metaphor.dbt.cloud.client.requests")
def test_list_projects(mock_requests):
    client = DbtAdminAPIClient(
        base_url="http://base.url",
        account_id=1111,
        service_token="service_token",
    )

    mock_requests.get.return_value = Response(
        200,
        {
            "data": [
                {
                    "id": 123,
                    "name": "test-project",
                    "account_id": 1111,
                    "connection_id": 456,
                    "created_at": "2023-01-01T00:00:00Z",
                    "updated_at": "2023-01-02T00:00:00Z",
                    "connection": {
                        "account_id": 1111,
                        "name": "test-conn",
                        "type": "snowflake",
                        "state": 1,
                        "details": {"account": "test_account"},
                    },
                }
            ]
        },
    )

    projects = client.list_projects()
    assert len(projects) == 1
    assert projects[0].id == 123
    assert projects[0].name == "test-project"
    assert projects[0].connection.type == "snowflake"

    mock_requests.get.assert_called_once_with(
        "http://base.url/api/v3/accounts/1111/projects/",
        params=None,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Token service_token",
        },
        timeout=60,
    )


@patch("metaphor.dbt.cloud.client.requests")
def test_list_environments(mock_requests):
    client = DbtAdminAPIClient(
        base_url="http://base.url",
        account_id=1111,
        service_token="service_token",
    )

    mock_requests.get.return_value = Response(
        200,
        {
            "data": [
                {
                    "id": 789,
                    "account_id": 1111,
                    "project_id": 123,
                    "name": "test-env",
                    "dbt_version": "1.5.0",
                    "type": "development",
                    "state": 1,
                    "created_at": "2023-01-01T00:00:00Z",
                    "updated_at": "2023-01-02T00:00:00Z",
                }
            ]
        },
    )

    environments = client.list_environments(123)
    assert len(environments) == 1
    assert environments[0].id == 789
    assert environments[0].name == "test-env"
    assert environments[0].dbt_version == "1.5.0"
    assert environments[0].type == "development"

    mock_requests.get.assert_called_once_with(
        "http://base.url/api/v3/accounts/1111/projects/123/environments/",
        params=None,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Token service_token",
        },
        timeout=60,
    )


def test_extract_platform_and_account():
    # Test Snowflake platform
    project = DbtProject.model_validate(
        {
            "id": 123,
            "name": "test-project",
            "account_id": 1111,
            "connection_id": 456,
            "created_at": "2023-01-01T00:00:00Z",
            "updated_at": "2023-01-02T00:00:00Z",
            "connection": {
                "account_id": 1111,
                "name": "test-conn",
                "type": "snowflake",
                "state": 1,
                "details": {"account": "test.snowflake.account"},
            },
        }
    )

    platform, account = extract_platform_and_account(project)
    assert platform == DataPlatform.SNOWFLAKE
    assert account == "test.snowflake.account"

    # Test PostgreSQL platform
    project = DbtProject.model_validate(
        {
            "id": 123,
            "name": "test-project",
            "account_id": 1111,
            "connection_id": 456,
            "created_at": "2023-01-01T00:00:00Z",
            "updated_at": "2023-01-02T00:00:00Z",
            "connection": {
                "account_id": 1111,
                "name": "test-conn",
                "type": "postgres",
                "state": 1,
                "details": {},
            },
        }
    )

    platform, account = extract_platform_and_account(project)
    assert platform == DataPlatform.POSTGRESQL
    assert account is None
