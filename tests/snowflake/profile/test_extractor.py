from unittest.mock import patch

from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.common.sampling import SamplingConfig
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    EntityType,
    FieldStatistics,
)
from metaphor.snowflake.profile.config import (
    ColumnStatistics,
    SnowflakeProfileRunConfig,
)
from metaphor.snowflake.profile.extractor import SnowflakeProfileExtractor


def column_statistics_config():
    return ColumnStatistics(
        null_count=True,
        unique_count=True,
        min_value=True,
        max_value=True,
        avg_value=True,
        std_dev=True,
    )


def test_default_excludes():

    with patch("metaphor.snowflake.auth.connect"):
        extractor = SnowflakeProfileExtractor(
            SnowflakeProfileRunConfig(
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


def test_build_profiling_query():
    columns = [
        ("id", "STRING"),
        ("price", "FLOAT"),
        ("year", "NUMBER"),
    ]
    schema, name = "schema", "table"

    expected = (
        "SELECT COUNT(1), "
        'COUNT(DISTINCT "id"), COUNT(1) - COUNT("id"), '
        'COUNT(DISTINCT "price"), COUNT(1) - COUNT("price"), MIN("price"), MAX("price"), AVG("price"), STDDEV(CAST("price" as DOUBLE)), '
        'COUNT(DISTINCT "year"), COUNT(1) - COUNT("year"), MIN("year"), MAX("year"), AVG("year"), STDDEV(CAST("year" as DOUBLE)) '
        'FROM "schema"."table"'
    )

    assert (
        SnowflakeProfileExtractor._build_profiling_query(
            columns, schema, name, 0, column_statistics_config(), SamplingConfig()
        )
        == expected
    )


def test_build_profiling_query_with_sampling():
    columns = [
        ("id", "STRING"),
        ("price", "FLOAT"),
    ]
    schema, name = "schema", "table"

    expected = (
        "SELECT COUNT(1), "
        'COUNT(DISTINCT "id"), COUNT(1) - COUNT("id"), '
        'COUNT(DISTINCT "price"), COUNT(1) - COUNT("price"), MIN("price"), MAX("price"), AVG("price"), STDDEV(CAST("price" as DOUBLE)) '
        'FROM "schema"."table" SAMPLE SYSTEM (1)'
    )

    assert (
        SnowflakeProfileExtractor._build_profiling_query(
            columns,
            schema,
            name,
            100000000,
            column_statistics_config(),
            SamplingConfig(percentage=1, threshold=100000000),
        )
        == expected
    )


def test_parse_profiling_result():
    columns = [
        ("id", "STRING"),
        ("price", "FLOAT"),
        ("year", "NUMBER"),
    ]
    results = (
        # row count
        10,
        # id
        1,
        2,
        # price
        3,
        4,
        5,
        6,
        7,
        8,
        # year
        9,
        10,
        11,
        12,
        13,
        14,
    )
    dataset = SnowflakeProfileExtractor._init_dataset(account="a", full_name="foo")

    SnowflakeProfileExtractor._parse_profiling_result(
        columns, results, dataset, column_statistics_config()
    )

    assert dataset == Dataset(
        entity_type=EntityType.DATASET,
        field_statistics=DatasetFieldStatistics(
            field_statistics=[
                FieldStatistics(
                    distinct_value_count=1.0,
                    field_path="id",
                    nonnull_value_count=8.0,
                    null_value_count=2.0,
                ),
                FieldStatistics(
                    average=7.0,
                    distinct_value_count=3.0,
                    field_path="price",
                    max_value=6.0,
                    min_value=5.0,
                    nonnull_value_count=6.0,
                    null_value_count=4.0,
                    std_dev=8.0,
                ),
                FieldStatistics(
                    average=13.0,
                    distinct_value_count=9.0,
                    field_path="year",
                    max_value=12.0,
                    min_value=11.0,
                    nonnull_value_count=0.0,
                    null_value_count=10.0,
                    std_dev=14.0,
                ),
            ]
        ),
        logical_id=DatasetLogicalID(
            account="a", name="foo", platform=DataPlatform.SNOWFLAKE
        ),
    )
