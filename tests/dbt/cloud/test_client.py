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

    run = client.get_last_successful_run(2222)
    assert run
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
def test_no_successful_runs(mock_requests):
    client = DbtAdminAPIClient(
        base_url="http://base.url",
        account_id=1111,
        service_token="service_token",
    )

    mock_requests.get.return_value = Response(
        200,
        {"data": []},
    )

    run = client.get_last_successful_run(2222)
    assert not run

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
def test_get_last_successful_run_pagination(mock_requests):
    client = DbtAdminAPIClient(
        base_url="http://base.url",
        account_id=1111,
        service_token="service_token",
    )

    mock_requests.get.side_effect = [
        Response(
            200,
            {
                "data": [
                    {
                        "id": 1,
                        "project_id": 111,
                        "job_definition_id": 9999,
                        "status": 20,
                    },
                ]
            },
        ),
        Response(
            200,
            {
                "data": [
                    {
                        "id": 2,
                        "project_id": 222,
                        "job_definition_id": 8888,
                        "status": 10,
                    },
                ]
            },
        ),
    ]
    run = client.get_last_successful_run(2222, page_size=1)
    assert run
    assert run.project_id == 222
    assert run.run_id == 2
    assert run.job_id == 8888

    assert mock_requests.get.call_count == 2


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
def test_get_project_jobs(mock_requests):
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
                    "id": 3333,
                },
                {
                    "id": 2222,
                },
            ]
        },
    )
    jobs = client.get_project_jobs(4444)
    assert jobs == [3333, 2222]


@patch("metaphor.dbt.cloud.client.requests")
def test_job_is_included(mock_requests):
    client = DbtAdminAPIClient(
        base_url="http://base.url",
        account_id=1111,
        service_token="service_token",
        included_env_ids={1, 3},
    )

    def mock_get(url: str, **kwargs):
        job_id = int(url.rsplit("/", 1)[-1])
        if job_id == 1:
            return Response(
                200,
                {
                    "data": {
                        "environment_id": 1,
                    }
                },
            )
        elif job_id == 2:
            return Response(
                200,
                {
                    "data": {
                        "environment_id": 2,
                    }
                },
            )
        elif job_id == 3:
            return Response(
                200,
                {
                    "data": {
                        "environment_id": 4,
                    }
                },
            )
        return Response(404, {})

    mock_requests.get = mock_get

    for i in range(1, 4):
        included = client.is_job_included(i)
        if i == 1:
            assert included
        else:
            assert not included
