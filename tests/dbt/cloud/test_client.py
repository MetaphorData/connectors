from typing import Dict
from unittest.mock import patch

from metaphor.dbt.cloud.client import DbtAdminAPIClient


class Response:
    def __init__(self, status_code: int, json: Dict):
        self.status_code = status_code
        self._json = json

    def json(self) -> Dict:
        return self._json


@patch("metaphor.dbt.cloud.client.requests")
def test_get_last_successful_run(mock_requests):
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
                    "id": 1,
                    "project_id": 111,
                    "status": 20,
                },
                {
                    "id": 2,
                    "project_id": 222,
                    "status": 10,
                },
            ]
        },
    )

    (project_id, run_id) = client.get_last_successful_run(2222)
    assert project_id == 222
    assert run_id == 2

    mock_requests.get.assert_called_once_with(
        "http://base.url/api/v2/accounts/1111/runs/",
        params={
            "job_definition_id": 2222,
            "order_by": "-id",
            "limit": 50,
            "offset": 0,
        },
        headers={
            "Content-Type": "application/json",
            "Authorization": "Token service_token",
        },
        timeout=600,
    )


@patch("metaphor.dbt.cloud.client.requests")
def test_get_snowflake_account(mock_requests):
    client = DbtAdminAPIClient(
        base_url="http://base.url",
        account_id=1111,
        service_token="service_token",
    )

    mock_requests.get.return_value = Response(
        200,
        {
            "data": {
                "connection": {
                    "type": "snowflake",
                    "details": {"account": "snowflake_account"},
                }
            }
        },
    )

    account = client.get_snowflake_account(2222)
    assert account == "snowflake_account"

    mock_requests.get.assert_called_once_with(
        "http://base.url/api/v2/accounts/1111/projects/2222",
        params=None,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Token service_token",
        },
        timeout=600,
    )


@patch("metaphor.dbt.cloud.client.requests")
def test_get_run_artifact(mock_requests):
    client = DbtAdminAPIClient(
        base_url="http://base.url",
        account_id=1111,
        service_token="service_token",
    )

    mock_requests.get.return_value = Response(200, {"artifact": "json"})

    path = client.get_run_artifact(2222, 3333, "manifest.json")
    assert path.endswith("/2222-manifest.json")

    mock_requests.get.assert_called_once_with(
        "http://base.url/api/v2/accounts/1111/runs/3333/artifacts/manifest.json",
        params=None,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Token service_token",
        },
        timeout=600,
    )
