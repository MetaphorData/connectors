from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.informatica.config import InformaticaRunConfig
from metaphor.informatica.extractor import InformaticaExtractor
from tests.test_utils import load_json


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return


@patch("requests.get")
@patch("requests.post")
@pytest.mark.asyncio
async def test_extractor(
    mock_post_method: MagicMock, mock_get_method: MagicMock, test_root_dir: str
):
    mock_post_method.side_effect = [
        MockResponse(
            load_json(
                f"{test_root_dir}/informatica/responses/post_saas__public__core__v3__login_03b80555.json"
            )
        ),
    ]

    mock_get_method.side_effect = [
        MockResponse(
            load_json(
                f"{test_root_dir}/informatica/responses/get_saas__api__v2__connection___da2aeee5.json"
            )
        ),
        MockResponse(
            load_json(
                f"{test_root_dir}/informatica/responses/get_saas__api__v2__connection__01CY6H0B000000000002_550571c1.json"
            )
        ),
        MockResponse(
            load_json(
                f"{test_root_dir}/informatica/responses/get_saas__public__core__v3__objects_8d406d40.json"
            )
        ),
        MockResponse(
            load_json(
                f"{test_root_dir}/informatica/responses/get_saas__public__core__v3__objects_b96dd6a7.json"
            )
        ),
        MockResponse(
            load_json(
                f"{test_root_dir}/informatica/responses/get_saas__public__core__v3__objects_b34053ea.json"
            )
        ),
        MockResponse(
            load_json(
                f"{test_root_dir}/informatica/responses/get_saas__public__core__v3__objects__71e4gvVQh8lclejElAZ2qZ__references_d6d6c585.json"
            )
        ),
        MockResponse(
            load_json(
                f"{test_root_dir}/informatica/responses/get_saas__public__core__v3__objects__8WzoMOwPAofia8FrbiDU2e__references_6bb91724.json"
            )
        ),
        MockResponse(
            load_json(
                f"{test_root_dir}/informatica/responses/get_saas__public__core__v3__objects__gdm2ALESzgxjhPkzOGvns7__references_56133b21.json"
            )
        ),
        MockResponse(
            load_json(
                f"{test_root_dir}/informatica/responses/get_saas__api__v2__mapping__01CY6H1700000000000W_e499f963.json"
            )
        ),
    ]

    config = InformaticaRunConfig(
        output=OutputConfig(), base_url="", user="", password=""
    )
    extractor = InformaticaExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    import json

    print(json.dumps(events))

    assert events == load_json(f"{test_root_dir}/informatica/expected.json")
