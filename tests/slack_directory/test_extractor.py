from unittest.mock import patch

import pytest
from freezegun import freeze_time

from metaphor.common.event_util import EventUtil
from metaphor.slack_directory.extractor import SlackExtractor, SlackRunConfig
from tests.test_utils import load_json


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor(test_root_dir):
    config = SlackRunConfig(
        output=None,
        oauth_token="fake_token",
        include_restricted=False,
    )
    extractor = SlackExtractor()

    # @patch doesn't work for async func in py3.7: https://bugs.python.org/issue36996
    with patch(
        "metaphor.slack_directory.extractor.list_all_users"
    ) as mock_list_all_users:
        mock_list_all_users.return_value = [
            {
                "id": "slack_id",
                "team_id": "team_id",
                "name": "foo",
                "is_bot": False,
                "deleted": False,
                "is_restricted": False,
                "is_ultra_restricted": False,
                "profile": {"email": "foo@bar.com"},
            },
            # Restricted user should be filtered out
            {
                "id": "slack_id2",
                "team_id": "team_id",
                "name": "foo2",
                "is_bot": False,
                "deleted": False,
                "is_restricted": True,
                "is_ultra_restricted": False,
                "profile": {"email": "foo2@bar.com"},
            },
            # Bot user should be filtered out
            {
                "id": "slack_id3",
                "team_id": "team_id",
                "name": "bot",
                "is_bot": True,
                "deleted": False,
                "is_restricted": True,
                "is_ultra_restricted": False,
                "profile": {"email": "bot@bar.com"},
            },
        ]

        events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(f"{test_root_dir}/slack_directory/expected.json")
