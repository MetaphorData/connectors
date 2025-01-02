import json
from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.extractor import DbtCloudExtractor
from metaphor.dbt.config import MetaOwnership
from tests.dbt.cloud.mock_client import MockAdminClient, MockDiscoveryClient
from tests.test_utils import load_json


@pytest.mark.asyncio()
@patch("metaphor.dbt.cloud.extractor.DiscoveryAPIClient")
@patch("metaphor.dbt.cloud.extractor.DbtAdminAPIClient")
async def test_extractor(
    mock_admin_client: MagicMock,
    mock_discovery_client: MagicMock,
    test_root_dir: str,
):
    mock_admin_client.return_value = MockAdminClient(test_root_dir)
    mock_discovery_client.return_value = MockDiscoveryClient(test_root_dir)

    extractor = DbtCloudExtractor(
        DbtCloudConfig(
            output=OutputConfig(),
            account_id=123,
            service_token="tok",
            meta_key_tags="my_tags",
            meta_ownerships=[
                MetaOwnership(
                    meta_key="Business Owner",
                    ownership_type="Business Owner",
                    email_domain="metaphor.com",
                )
            ],
        )
    )
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    expected = f"{test_root_dir}/dbt/cloud/data/expected.json"
    print(json.dumps(events))
    assert events == load_json(expected)
