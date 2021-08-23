from unittest.mock import patch

import pytest
from freezegun import freeze_time

from metaphor.common.event_util import EventUtil
from metaphor.google_directory.extractor import (
    GoogleDirectoryExtractor,
    GoogleDirectoryRunConfig,
)
from tests.test_utils import load_json


def mock_users_list(mock_build_service, value):
    mock_build_service.return_value.users.return_value.list.return_value.execute.return_value = (
        value
    )


def mock_members_list(mock_build_service, value):
    mock_build_service.return_value.members.return_value.list.return_value.execute.return_value = (
        value
    )


def mock_users_photos_get(mock_build_service, value):
    mock_build_service.return_value.users.return_value.photos.return_value.get.return_value.execute.return_value = (
        value
    )


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor_user(test_root_dir):
    config = GoogleDirectoryRunConfig(output=None, token_file="fake_file")
    extractor = GoogleDirectoryExtractor()

    # @patch doesn't work for async func in py3.7: https://bugs.python.org/issue36996
    with patch(
        "metaphor.google_directory.extractor.build_directory_service",
    ) as mock_build_service:
        mock_users_list(
            mock_build_service,
            {
                "users": [
                    {
                        "id": "1234",
                        "primaryEmail": "foo@bar.com",
                        "name": {"givenName": "Foo", "familyName": "Bar"},
                    },
                    {
                        "id": "5678",
                        "primaryEmail": "foo2@bar.com",
                        "name": {"givenName": "Foo2", "familyName": "Bar2"},
                        "organizations": [
                            {
                                "title": "software engineer",
                                "department": "engineering",
                            }
                        ],
                    },
                ]
            },
        )

        mock_users_photos_get(
            mock_build_service,
            {
                "photoData": "1111",
                "mimeType": "image/jpeg",
            },
        )

        events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(f"{test_root_dir}/google_directory/expected_users.json")
