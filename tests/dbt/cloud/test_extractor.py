from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.extractor import DbtCloudExtractor
from metaphor.dbt.config import DbtRunConfig


@pytest.mark.asyncio
async def test_extractor(test_root_dir):

    mock_client = MagicMock()
    mock_client.get_last_successful_run = MagicMock(return_value=(3333, 4444))
    mock_client.get_snowflake_account = MagicMock(return_value="snowflake_account")
    mock_client.get_run_artifact = MagicMock(return_value="tempfile")

    async def fake_extract(config):
        return []

    mock_dbt_extractor = MagicMock()
    mock_dbt_extractor.extract.side_effect = fake_extract

    with (
        patch("metaphor.dbt.cloud.extractor.DbtAdminAPIClient") as mock_client_class,
        patch("metaphor.dbt.cloud.extractor.DbtExtractor") as mock_dbt_extractor_class,
    ):
        mock_client_class.return_value = mock_client
        mock_dbt_extractor_class.return_value = mock_dbt_extractor

        config = DbtCloudConfig(
            output=OutputConfig(),
            account_id=1111,
            job_id=2222,
            service_token="service_token",
        )
        extractor = DbtCloudExtractor()
        await extractor.extract(config)

        mock_dbt_extractor.extract.assert_called_once_with(
            DbtRunConfig(
                manifest="tempfile",
                account="snowflake_account",
                docs_base_url="https://cloud.getdbt.com/accounts/1111/runs/4444/docs",
                output=OutputConfig(),
            )
        )
