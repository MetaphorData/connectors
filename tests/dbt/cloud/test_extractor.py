from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.dbt.cloud.client import DbtRun
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.discovery_api import DiscoveryTestNode
from metaphor.dbt.cloud.extractor import DbtCloudExtractor
from metaphor.models.metadata_change_event import (
    DataMonitorStatus,
    DataMonitorTarget,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)


@patch("metaphor.dbt.cloud.extractor.DiscoveryAPI")
@patch("metaphor.dbt.cloud.extractor.get_data_platform_from_manifest")
@patch("metaphor.dbt.cloud.extractor.DbtAdminAPIClient")
@patch("metaphor.dbt.cloud.extractor.DbtExtractor")
@pytest.mark.asyncio
async def test_extractor(
    mock_dbt_extractor_class: MagicMock,
    mock_client_class: MagicMock,
    mock_get_data_platform_from_manifest: MagicMock,
    mock_discovery_api_class: MagicMock,
):
    mock_client = MagicMock()
    mock_client.get_last_successful_run = MagicMock(
        side_effect=(
            DbtRun(run_id=3333, project_id=4444, job_id=2222),
            DbtRun(run_id=7777, project_id=6666, job_id=8888),
            DbtRun(run_id=3333, project_id=4444, job_id=2222),
        )
    )
    mock_client.get_project_jobs = MagicMock(side_effect=[[8888], [2222]])

    def mock_is_job_included(job_id: int) -> bool:
        return job_id != 3333

    mock_client.is_job_included = mock_is_job_included
    mock_client.get_snowflake_account = MagicMock(return_value="snowflake_account")
    mock_client.get_run_artifact = MagicMock(return_value="tempfile")

    mock_get_data_platform_from_manifest.return_value = DataPlatform.UNKNOWN

    mock_dbt_extractor = MagicMock()

    async def fake_extract():
        return []

    mock_dbt_extractor.extract.side_effect = fake_extract

    mock_client_class.return_value = mock_client
    mock_dbt_extractor_class.return_value = mock_dbt_extractor

    mock_discovery_api = MagicMock()
    mock_discovery_api.get_all_job_tests.return_value = []

    config = DbtCloudConfig(
        output=OutputConfig(),
        account_id=1111,
        job_ids={2222, 3333},
        project_ids={6666, 4444},
        environment_ids={1},
        base_url="https://cloud.metaphor.getdbt.com",
        service_token="service_token",
    )
    extractor = DbtCloudExtractor(config)
    await extractor.extract()
    assert sorted(extractor._entities.keys()) == [3333, 7777]


@patch("metaphor.dbt.cloud.extractor.get_data_platform_from_manifest")
@patch("metaphor.dbt.cloud.extractor.DbtAdminAPIClient")
@patch("metaphor.dbt.cloud.extractor.DbtExtractor")
@pytest.mark.asyncio
async def test_extractor_bad_source(
    mock_dbt_extractor_class: MagicMock,
    mock_client_class: MagicMock,
    mock_get_data_platform_from_manifest: MagicMock,
):
    mock_client = MagicMock()
    mock_client.get_last_successful_run = MagicMock(
        side_effect=(
            DbtRun(run_id=3333, project_id=4444, job_id=2222),
            DbtRun(run_id=7777, project_id=6666, job_id=8888),
            DbtRun(run_id=3333, project_id=4444, job_id=2222),
        )
    )
    mock_client.get_project_jobs = MagicMock(side_effect=[[8888], [2222]])
    mock_client.get_snowflake_account = MagicMock(return_value="snowflake_account")
    mock_client.get_run_artifact = MagicMock(return_value="tempfile")

    mock_get_data_platform_from_manifest.return_value = DataPlatform.UNKNOWN

    mock_dbt_extractor = MagicMock()

    async def fake_extract():
        raise ValueError()

    mock_dbt_extractor.extract.side_effect = fake_extract

    mock_client_class.return_value = mock_client
    mock_dbt_extractor_class.return_value = mock_dbt_extractor

    config = DbtCloudConfig(
        output=OutputConfig(),
        account_id=1111,
        job_ids={2222},
        project_ids={6666, 4444},
        base_url="https://cloud.metaphor.getdbt.com",
        service_token="service_token",
    )
    extractor = DbtCloudExtractor(config)
    await extractor.extract()
    assert not extractor._entities


@patch("metaphor.dbt.cloud.extractor.DiscoveryAPI")
def test_extend_test_run_results_entities(mock_discovery_api_class: MagicMock):
    config = DbtCloudConfig(
        output=OutputConfig(),
        account_id=1111,
        job_ids={2222},
        project_ids={6666, 4444},
        base_url="https://cloud.metaphor.getdbt.com",
        service_token="service_token",
    )
    extractor = DbtCloudExtractor(config)
    mock_discovery_api = MagicMock()
    mock_discovery_api.get_all_job_model_names.return_value = {
        "model.foo.bar": "db.sch.tab"
    }

    def fake_get_all_job_tests(job_id: int):
        return [
            DiscoveryTestNode(
                uniqueId="1",
                name="test1",
                columnName="col1",
                status="pass",
                executeCompletedAt=datetime.now(),
                dependsOn=["model.foo.bar"],
            ),
            DiscoveryTestNode(
                uniqueId="2",
                name="test2",
                columnName="col2",
                status="error",
                executeCompletedAt=datetime.now(),
                dependsOn=["model.foo.bar"],
            ),
        ]

    mock_discovery_api.get_all_job_tests.side_effect = fake_get_all_job_tests
    mock_discovery_api_class.return_value = mock_discovery_api
    entities = [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="foo.bar",
                type=VirtualViewType.DBT_MODEL,
            ),
        ),
        Dataset(
            logical_id=DatasetLogicalID(
                name="a.b.c",
                platform=DataPlatform.UNKNOWN,
            )
        ),
    ]

    res = extractor._extend_test_run_results_entities(
        DataPlatform.UNKNOWN, None, 2222, entities
    )
    assert len(res) == 3
    dataset = next(
        x for x in res if isinstance(x, Dataset) and x.data_quality is not None
    )
    assert dataset.data_quality and dataset.data_quality.monitors
    assert dataset.data_quality.monitors[0].status == DataMonitorStatus.PASSED
    assert dataset.data_quality.monitors[0].targets == [
        DataMonitorTarget(
            column="col1", dataset="DATASET~083603875008F6D0B4981A524F67A549"
        )
    ]
    assert dataset.data_quality.monitors[1].status == DataMonitorStatus.ERROR
    assert dataset.data_quality.monitors[1].targets == [
        DataMonitorTarget(
            column="col2", dataset="DATASET~083603875008F6D0B4981A524F67A549"
        )
    ]
