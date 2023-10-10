from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.extractor import DbtCloudExtractor
from metaphor.dbt.config import DbtRunConfig


@patch("metaphor.dbt.cloud.extractor.DbtAdminAPIClient")
@patch("metaphor.dbt.cloud.extractor.DbtExtractor")
@pytest.mark.asyncio
async def test_extractor(
    mock_dbt_extractor_class: MagicMock, mock_client_class: MagicMock
):
    mock_client = MagicMock()
    mock_client.get_last_successful_run = MagicMock(return_value=(3333, 4444))
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
        base_url="https://cloud.metaphor.getdbt.com",
        service_token="service_token",
    )
    extractor = DbtCloudExtractor(config)
    await extractor.extract()

    mock_client_class.assert_called_once_with(
        base_url="https://cloud.metaphor.getdbt.com",
        account_id=1111,
        service_token="service_token",
    )

    mock_dbt_extractor_class.assert_called_once_with(
        DbtRunConfig(
            manifest="tempfile",
            account="snowflake_account",
            docs_base_url="https://cloud.metaphor.getdbt.com/accounts/1111/jobs/2222/docs",
            output=OutputConfig(),
        )
    )
    mock_dbt_extractor.extract.assert_called_once()
