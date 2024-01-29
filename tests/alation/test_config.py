from metaphor.alation.config import AlationConfig
from metaphor.common.base_config import OutputConfig
from metaphor.models.metadata_change_event import DataPlatform


def test_config() -> None:
    config = AlationConfig(
        output=OutputConfig(),
        url="url",
        token="token",
        snowflake_account="snowflake account",
        mssql_account="mssql account",
        synapse_account="synapse account",
    )

    assert config.get_account(DataPlatform.SNOWFLAKE) == "snowflake account"
    assert config.get_account(DataPlatform.MSSQL) == "mssql account"
    assert config.get_account(DataPlatform.SYNAPSE) == "synapse account"
    assert not config.get_account(DataPlatform.BIGQUERY)
