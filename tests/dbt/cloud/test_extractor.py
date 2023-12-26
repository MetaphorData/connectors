from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.dbt.cloud.client import DbtRun
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.extractor import DbtCloudExtractor
from metaphor.models.crawler_run_metadata import RunStatus


@patch("metaphor.dbt.cloud.extractor.DbtAdminAPIClient")
@patch("metaphor.dbt.cloud.extractor.DbtExtractor")
@pytest.mark.asyncio
async def test_extractor(
    mock_dbt_extractor_class: MagicMock, mock_client_class: MagicMock
):
    mock_client = MagicMock()
    mock_client.get_last_successful_run = MagicMock(
        side_effect=(
            DbtRun(run_id=3333, project_id=4444, job_id=2222),
            DbtRun(run_id=7777, project_id=6666, job_id=8888),
            DbtRun(run_id=3333, project_id=4444, job_id=2222),  # This is skipped
        )
    )
    mock_client.get_snowflake_account = MagicMock(return_value="snowflake_account")
    mock_client.get_run_artifact = MagicMock(return_value="tempfile")

    mock_dbt_extractor = MagicMock()

    async def fake_extract():
        return []

    mock_dbt_extractor.extract.side_effect = fake_extract

    mock_client_class.return_value = mock_client
    mock_dbt_extractor_class.return_value = mock_dbt_extractor

    config = DbtCloudConfig(
        output=OutputConfig(),
        account_id=1111,
        job_ids=[2222],
        project_ids=[6666, 4444],
        base_url="https://cloud.metaphor.getdbt.com",
        service_token="service_token",
    )
    extractor = DbtCloudExtractor(config)
    await extractor.extract()
    assert sorted(extractor._entities.keys()) == [3333, 7777]


@patch("metaphor.dbt.cloud.extractor.DbtAdminAPIClient")
@patch("metaphor.dbt.cloud.extractor.DbtExtractor")
@pytest.mark.asyncio
async def test_extractor_not_throwing_exception(
    mock_dbt_extractor_class: MagicMock, mock_client_class: MagicMock
):
    mock_client = MagicMock()
    mock_client.get_last_successful_run = MagicMock(
        side_effect=(DbtRun(run_id=0, project_id=1, job_id=2),)
    )
    mock_client.get_snowflake_account = MagicMock(return_value="snowflake_account")
    mock_client.get_run_artifact = MagicMock(return_value="tempfile")

    mock_dbt_extractor = MagicMock()

    async def fake_extract():
        raise ValueError("GG")

    mock_dbt_extractor.extract.side_effect = fake_extract

    mock_client_class.return_value = mock_client
    mock_dbt_extractor_class.return_value = mock_dbt_extractor

    config = DbtCloudConfig(
        output=OutputConfig(),
        account_id=1111,
        job_ids=[2],
        base_url="https://cloud.metaphor.getdbt.com",
        service_token="service_token",
    )
    extractor = DbtCloudExtractor(config)
    await extractor.extract()  # Note how it does not break even when the Dbt core extractor is throwing exceptions
    assert not extractor._entities
    assert extractor.status is RunStatus.FAILURE
    assert extractor.error_message == "GG"
