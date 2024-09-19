from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.quick_sight.config import AwsCredentials, QuickSightRunConfig
from metaphor.quick_sight.extractor import QuickSightExtractor
from tests.test_utils import load_json


def dummy_config():
    return QuickSightRunConfig(
        aws=AwsCredentials(
            access_key_id="key", secret_access_key="secret", region_name="region"
        ),
        aws_account_id=123,
        output=OutputConfig(),
    )


@patch("metaphor.quick_sight.extractor.create_quick_sight_client")
@pytest.mark.asyncio
async def test_extractor(mock_create_client: MagicMock, test_root_dir: str):
    datasets_response = load_json(f"{test_root_dir}/quick_sight/data/datasets.json")
    dashboards_response = load_json(f"{test_root_dir}/quick_sight/data/dashboards.json")
    data_sources_response = load_json(
        f"{test_root_dir}/quick_sight/data/data_sources.json"
    )

    list_data_sets_response = [
        {
            "DataSetSummaries": [
                {"DataSetId": item["DataSet"]["DataSetId"]}
                for item in datasets_response
            ]
        }
    ]

    list_dashboards_response = [
        {
            "DashboardSummaryList": [
                {"DashboardId": item["Dashboard"]["DashboardId"]}
                for item in dashboards_response
            ]
        }
    ]

    list_data_sources_response = [
        {
            "DataSources": [
                {"DataSourceId": item["DataSource"]["DataSourceId"]}
                for item in data_sources_response
            ]
        }
    ]

    def mock_describe_data_set(DataSetId: str, AwsAccountId):
        return next(
            item
            for item in datasets_response
            if item["DataSet"]["DataSetId"] == DataSetId
        )

    def mock_describe_dashboard(DashboardId: str, AwsAccountId):
        return next(
            item
            for item in dashboards_response
            if item["Dashboard"]["DashboardId"] == DashboardId
        )

    def mock_describe_data_source(DataSourceId: str, AwsAccountId):
        return next(
            item
            for item in data_sources_response
            if item["DataSource"]["DataSourceId"] == DataSourceId
        )

    def mock_get_paginator(method: str):
        if method == "list_data_sets":
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = list_data_sets_response
            return mock_paginator
        elif method == "list_dashboards":
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = list_dashboards_response
            return mock_paginator
        elif method == "list_data_sources":
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = list_data_sources_response
            return mock_paginator

    mock_client = MagicMock()
    mock_client.get_paginator = mock_get_paginator
    mock_client.describe_data_set = mock_describe_data_set
    mock_client.describe_dashboard = mock_describe_dashboard
    mock_client.describe_data_source = mock_describe_data_source
    mock_create_client.return_value = mock_client

    extractor = QuickSightExtractor(dummy_config())
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/quick_sight/expected.json")
