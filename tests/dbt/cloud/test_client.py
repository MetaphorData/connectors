from typing import Dict
from unittest.mock import patch

from metaphor.dbt.cloud.client import DbtAdminAPIClient, DbtRun


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
                    "job_definition_id": 9999,
                    "status": 20,
                },
                {
                    "id": 2,
                    "project_id": 222,
                    "job_definition_id": 8888,
                    "status": 10,
                },
            ]
        },
    )

    run = client.get_last_successful_run(2222, None)
    assert run.project_id == 222
    assert run.run_id == 2
    assert run.job_id == 8888

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

    run = DbtRun(run_id=2222, project_id=3333, job_id=4444)
    path = client.get_run_artifact(run, "manifest.json")
    assert path.endswith("/3333-4444-manifest.json")

    mock_requests.get.assert_called_once_with(
        "http://base.url/api/v2/accounts/1111/runs/2222/artifacts/manifest.json",
        params=None,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Token service_token",
        },
        timeout=600,
    )
