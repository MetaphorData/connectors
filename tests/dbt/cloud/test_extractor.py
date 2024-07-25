import json
from typing import List, Set
from unittest.mock import MagicMock, patch

import pytest
from httpx import Response

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.dbt.cloud.client import DbtRun
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.extractor import DbtCloudExtractor
from tests.dbt.cloud.fake_graphql_server import endpoints
from tests.test_utils import load_json


def mock_post(url: str, content: str, **kwargs):
    content_json = json.loads(content)
    operation_name = content_json["operationName"]
    variables = content_json["variables"]
    results = endpoints[operation_name](variables)
    payload = {"data": json.loads(results)}
    return Response(200, content=json.dumps(payload))


class MockAdminClient:
    def __init__(
        self,
        base_url: str,
        account_id: int,
        service_token: str,
        included_env_ids: Set[int] = set(),
    ):
        pass

    def get_project_jobs(self, project_id: int) -> List[int]:
        return [123]

    def is_job_included(self, job_id: int):
        return True

    def get_last_successful_run(self, job_id: int, page_size=50):
        return DbtRun(
            123,
            job_id,
            1234,
            12345,
        )

    def get_snowflake_account(self, project_id: int):
        return "john.doe@metaphor.io"


@pytest.mark.asyncio()
@patch("metaphor.dbt.cloud.extractor.DbtAdminAPIClient")
@patch("httpx.Client.post")
async def test_extractor(
    mock_httpx_client_post: MagicMock,
    mock_admin_client: MagicMock,
    test_root_dir: str,
):
    mock_httpx_client_post.side_effect = mock_post
    mock_admin_client.side_effect = MockAdminClient

    extractor = DbtCloudExtractor(
        DbtCloudConfig(
            output=OutputConfig(),
            account_id=1,
            service_token="tok",
            project_ids={123},
        )
    )
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    expected = f"{test_root_dir}/dbt/cloud/expected.json"
    assert events == load_json(expected)
