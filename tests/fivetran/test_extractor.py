from unittest.mock import patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.fivetran.config import FivetranRunConfig
from metaphor.fivetran.extractor import FivetranExtractor
from tests.test_utils import load_json


def dummy_config():
    return FivetranRunConfig(
        api_key="key",
        api_secret="secret",
        output=OutputConfig(),
    )


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


@pytest.mark.asyncio
async def test_extractor(test_root_dir):
    with patch("requests.get") as mock_get:
        mock_get.side_effect = [
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__metadata__connectors.json"
                )
            ),
            MockResponse(load_json(f"{test_root_dir}/fivetran/data/v1__groups.json")),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__destinations__group_id_1.json"
                )
            ),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__groups__group_id_1__connectors.json"
                )
            ),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__groups__group_id_1__connectors_2.json"
                )
            ),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__connectors__connector_1.json"
                )
            ),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__connectors__connector_2.json"
                )
            ),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__metadata__connectors__connector_1__schemas.json"
                )
            ),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__metadata__connectors__connector_1__tables.json"
                )
            ),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__metadata__connectors__connector_1__columns.json"
                )
            ),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__metadata__connectors__connector_2__schemas.json"
                )
            ),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__metadata__connectors__connector_2__tables.json"
                )
            ),
            MockResponse(
                load_json(
                    f"{test_root_dir}/fivetran/data/v1__metadata__connectors__connector_2__columns.json"
                )
            ),
        ]

        extractor = FivetranExtractor(dummy_config())
        events = [EventUtil.trim_event(e) for e in await extractor.extract()]

        import json

        print(json.dumps(events))

    assert events == load_json(f"{test_root_dir}/fivetran/expected.json")
