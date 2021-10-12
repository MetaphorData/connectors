from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    EntityType,
    FieldStatistics,
)

from metaphor.snowflake.profile.extractor import SnowflakeProfileExtractor


def test_build_profiling_query():
    columns = [
        ("id", "STRING", False),
        ("price", "FLOAT", True),
        ("year", "NUMBER", True),
    ]
    schema, name = "schema", "table"

    expected = (
        'SELECT COUNT(1) ROW_COUNT, COUNT(DISTINCT "id"), COUNT(DISTINCT "price"), '
        'COUNT_IF("price" is NULL), MIN("price"), MAX("price"), AVG("price"), STDDEV("price"), '
        'COUNT(DISTINCT "year"), COUNT_IF("year" is NULL), MIN("year"), MAX("year"), AVG("year"), '
        'STDDEV("year") '
        "FROM schema.table"
    )

    assert (
        SnowflakeProfileExtractor._build_profiling_query(columns, schema, name)
        == expected
    )


def test_parse_profiling_result():
    columns = [
        ("id", "STRING", False),
        ("price", "FLOAT", True),
        ("year", "NUMBER", True),
    ]
    results = (5, 5, 4, 0, 3, 8, 5, 1.5, 2, 1, 2000, 2020, 2015, 2.3)
    dataset = SnowflakeProfileExtractor._init_dataset(account="a", full_name="foo")

    SnowflakeProfileExtractor._parse_profiling_result(columns, results, dataset)

    assert dataset == Dataset(
        entity_type=EntityType.DATASET,
        field_statistics=DatasetFieldStatistics(
            field_statistics=[
                FieldStatistics(
                    distinct_value_count=5.0,
                    field_path="id",
                    nonnull_value_count=5.0,
                    null_value_count=0.0,
                ),
                FieldStatistics(
                    average=5.0,
                    distinct_value_count=4.0,
                    field_path="price",
                    max_value=8.0,
                    min_value=3.0,
                    nonnull_value_count=5.0,
                    null_value_count=0.0,
                    std_dev=1.5,
                ),
                FieldStatistics(
                    average=2015.0,
                    distinct_value_count=2.0,
                    field_path="year",
                    max_value=2020.0,
                    min_value=2000.0,
                    nonnull_value_count=4.0,
                    null_value_count=1.0,
                    std_dev=2.3,
                ),
            ]
        ),
        logical_id=DatasetLogicalID(
            account="a", name="foo", platform=DataPlatform.SNOWFLAKE
        ),
    )
