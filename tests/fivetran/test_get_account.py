from metaphor.fivetran.extractor import FivetranExtractor
from metaphor.models.metadata_change_event import DataPlatform


def test_get_account():
    assert (
        FivetranExtractor.get_source_account_from_config({}, DataPlatform.BIGQUERY)
        is None
    )

    assert (
        FivetranExtractor.get_source_account_from_config(
            {"host": "Test-Sqldb.Domain.com"}, DataPlatform.MSSQL
        )
        == "test-sqldb.domain.com"
    )

    assert (
        FivetranExtractor.get_source_account_from_config(
            {"host": "account.snowflakecomputing.com"}, DataPlatform.SNOWFLAKE
        )
        == "account"
    )
