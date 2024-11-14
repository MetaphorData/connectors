from collections import OrderedDict
from typing import Optional
from unittest.mock import MagicMock, call, patch

import pytest
import requests
from pydantic import FilePath, HttpUrl

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.openapi.config import OpenAPIJsonConfig, OpenAPIRunConfig
from metaphor.openapi.extractor import OpenAPIExtractor
from tests.test_utils import load_json


def get_dummy_config(base_url: HttpUrl, path: str = "", url: Optional[HttpUrl] = None):
    return OpenAPIRunConfig(
        base_url=base_url,
        openapi_json_path=FilePath(path) if path else None,
        openapi_json_url=url,
        output=OutputConfig(),
    )


@pytest.mark.parametrize(
    ["data_folder", "base_url"],
    [
        ("pet_store_31", "https://foo.bar"),
        ("pet_store_30", "https://foo.bar"),
        ("pet_store_20", "https://foo.bar"),
        ("pet_store_31_absolute_server", "https://foo.bar"),
        ("no_tag_30", "https://foo.bar"),
    ],
)
@pytest.mark.asyncio
async def test_extractor(data_folder, base_url, test_root_dir):
    data_folder = f"{test_root_dir}/openapi/data/{data_folder}"

    extractor = OpenAPIExtractor(
        config=get_dummy_config(
            base_url,
            path=f"{data_folder}/openapi.json",
        )
    )
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{data_folder}/expected.json")


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return


@patch.object(requests.Session, "get")
def test_get_openapi_json(mock_get_method: MagicMock):
    mock_get_method.side_effect = [
        MockResponse({}, 403),
        MockResponse({}, 200),
    ]
    base_url = HttpUrl("http://baz")
    openapi_url = HttpUrl("http://foo.bar/openapi.json")
    extractor = OpenAPIExtractor(get_dummy_config(base_url, url=openapi_url))

    assert (
        extractor._get_openapi_json(
            OpenAPIJsonConfig(base_url, openapi_json_url=openapi_url)
        )
        is None
    )
    assert (
        extractor._get_openapi_json(
            OpenAPIJsonConfig(base_url, openapi_json_url=openapi_url)
        )
        == {}
    )

    mock_get_method.assert_has_calls(
        [
            call(
                "http://foo.bar/openapi.json",
                headers=OrderedDict(
                    [
                        ("User-Agent", None),
                        ("Accept", "application/json"),
                        ("Connection", None),
                        ("Accept-Encoding", None),
                    ]
                ),
            ),
            call(
                "http://foo.bar/openapi.json",
                headers=OrderedDict(
                    [
                        ("User-Agent", None),
                        ("Accept", "application/json"),
                        ("Connection", None),
                        ("Accept-Encoding", None),
                    ]
                ),
            ),
        ]
    )
