from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.models.metadata_change_event import DataPlatform
from metaphor.monte_carlo.config import MonteCarloRunConfig
from metaphor.monte_carlo.extractor import MonteCarloExtractor
from tests.test_utils import load_json


def dummy_config():
    return MonteCarloRunConfig(
        api_key_id="key_id",
        api_key_secret="key_secret",
        data_platform=DataPlatform.SNOWFLAKE,
        snowflake_account="snow",
        output=OutputConfig(),
    )


@patch("pycarlo.core.Client")
@pytest.mark.asyncio
async def test_extractor(mock_pycarlo_client: MagicMock, test_root_dir: str):
    mock_pycarlo_client.return_value = {
        "get_monitors": [
            {
                "uuid": "e0dc143e-dd8a-4cb9-b4cc-dedec715d955",
                "name": "auto_monitor_name_cd5b69bd-e465-4545-b3f9-a5d507ea766c",
                "description": "Field Health for all fields in db:metaphor.test1",
                "entities": ["db:metaphor.test1"],
                "severity": None,
                "monitorStatus": "MISCONFIGURED",
                "monitorFields": None,
                "creatorId": "yi@metaphor.io",
                "prevExecutionTime": "2023-06-23T03:54:35.817000+00:00",
            },
            {
                "uuid": "ce4c4568-35f4-4365-a6fe-95f233fcf6c3",
                "name": "auto_monitor_name_53c985e6-8f49-4af7-8ef1-7b402a27538b",
                "description": "Field Health for all fields in db:metaphor.test2",
                "entities": ["db:metaphor.test2"],
                "severity": "LOW",
                "monitorStatus": "SUCCESS",
                "monitorFields": ["foo", "bar"],
                "creatorId": "yi@metaphor.io",
                "prevExecutionTime": "2023-06-23T03:54:35.817000+00:00",
            },
        ]
    }

    extractor = MonteCarloExtractor(dummy_config())
    extractor._client = mock_pycarlo_client

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/monte_carlo/expected.json")