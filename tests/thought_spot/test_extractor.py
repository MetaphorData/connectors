from unittest.mock import MagicMock, patch

import pytest
import requests

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.thought_spot.config import ThoughtSpotRunConfig
from metaphor.thought_spot.extractor import ThoughtSpotExtractor
from tests.test_utils import load_json


def dummy_config():
    return ThoughtSpotRunConfig(
        user="user",
        password="password",
        base_url="http://base.url",
        output=OutputConfig(),
    )


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return


@patch.object(requests.Session, "post")
@pytest.mark.asyncio
async def test_extractor(mock_post_method: MagicMock, test_root_dir: str):
    mock_post_method.side_effect = [
        MockResponse({"token": ""}),
        MockResponse(
            load_json(f"{test_root_dir}/thought_spot/data/connections.json"),
        ),
        MockResponse(
            load_json(f"{test_root_dir}/thought_spot/data/data_objects.json"),
        ),
        MockResponse(load_json(f"{test_root_dir}/thought_spot/data/tml.json")),
        MockResponse(
            load_json(f"{test_root_dir}/thought_spot/data/answers.json"),
        ),
        MockResponse(load_json(f"{test_root_dir}/thought_spot/data/tml_answer.json")),
        MockResponse(load_json(f"{test_root_dir}/thought_spot/data/answer_sql.json")),
        MockResponse(
            load_json(f"{test_root_dir}/thought_spot/data/liveboards.json"),
        ),
    ]
    extractor = ThoughtSpotExtractor(dummy_config())
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    expected = f"{test_root_dir}/thought_spot/expected.json"
    assert events == load_json(expected)
