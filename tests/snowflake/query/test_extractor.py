from unittest.mock import patch

from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.query.config import SnowflakeQueryRunConfig
from metaphor.snowflake.query.extractor import SnowflakeQueryExtractor


def test_default_excludes():

    with patch("metaphor.snowflake.auth.connect"):
        extractor = SnowflakeQueryExtractor(
            SnowflakeQueryRunConfig(
                account="snowflake_account",
                user="user",
                password="password",
                filter=DatasetFilter(
                    includes={"foo": None},
                    excludes={"bar": None},
                ),
                output=OutputConfig(),
            )
        )

        assert extractor._filter.includes == {"foo": None}
        assert extractor._filter.excludes == {
            "bar": None,
            "SNOWFLAKE": None,
            "*": {"INFORMATION_SCHEMA": None},
        }
