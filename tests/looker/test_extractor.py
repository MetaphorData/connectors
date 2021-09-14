from metaphor.models.metadata_change_event import DataPlatform

from metaphor.looker.extractor import LookerExtractor, LookerRunConfig


def test_parse_account(test_root_dir):
    assert (
        LookerExtractor.parse_account(
            "account.snowflakecomputing.com", DataPlatform.SNOWFLAKE
        )
        == "account"
    )

    assert LookerExtractor.parse_account("account", DataPlatform.SNOWFLAKE) == "account"
    assert LookerExtractor.parse_account("account", DataPlatform.POSTGRESQL) is None
