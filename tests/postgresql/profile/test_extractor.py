from metaphor.common.column_statistics import ColumnStatistics
from metaphor.common.sampling import SamplingConfig
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStatistics,
    FieldStatistics,
    SchemaField,
)
from metaphor.postgresql.profile.extractor import PostgreSQLProfileExtractor

column_statistics = ColumnStatistics(unique_count=True, avg_value=True)


def init_dataset(name: str, row_count) -> Dataset:
    dataset = Dataset()
    dataset.logical_id = DatasetLogicalID()
    dataset.logical_id.platform = DataPlatform.POSTGRESQL
    dataset.logical_id.name = name

    dataset.schema = DatasetSchema()
    dataset.schema.fields = []

    dataset.statistics = DatasetStatistics()
    dataset.statistics.record_count = float(row_count)
    return dataset


def test_build_profiling_query():
    dataset = init_dataset(name="foo", row_count=1000)
    dataset.schema.fields = [
        SchemaField(field_path="id", field_name="id", nullable=False, subfields=None),
        SchemaField(
            field_path="price",
            field_name="price",
            nullable=True,
            precision=22.0,
            subfields=None,
        ),
        SchemaField(
            field_path="year",
            field_name="year",
            nullable=True,
            precision=32.0,
            subfields=None,
        ),
    ]

    expected = [
        (
            'SELECT COUNT(1), COUNT(DISTINCT "id"), COUNT(DISTINCT "price"), '
            'COUNT("price"), MIN("price"), MAX("price"), AVG("price"::double precision), '
            'COUNT(DISTINCT "year"), COUNT("year"), MIN("year"), MAX("year"), AVG("year"::double precision) '
            "FROM foo"
        )
    ]

    assert (
        PostgreSQLProfileExtractor._build_profiling_query(
            dataset, column_statistics, SamplingConfig()
        )
        == expected
    )


def test_build_profiling_query_multiple_sql():
    dataset = init_dataset(name="foo", row_count=1000)
    dataset.schema.fields = [
        SchemaField(field_path="id", field_name="id", nullable=False, subfields=None),
        SchemaField(
            field_path="price",
            field_name="price",
            nullable=True,
            precision=22.0,
            subfields=None,
        ),
        SchemaField(
            field_path="year",
            field_name="year",
            nullable=True,
            precision=32.0,
            subfields=None,
        ),
        SchemaField(
            field_path="name", field_name="name", nullable=True, subfields=None
        ),
    ]

    expected = [
        (
            "SELECT "
            'COUNT(1), COUNT(DISTINCT "id"), COUNT(DISTINCT "price"), '
            'COUNT("price"), MIN("price") '
            "FROM foo"
        ),
        (
            "SELECT "
            'MAX("price"), AVG("price"::double precision), COUNT(DISTINCT "year"), '
            'COUNT("year"), MIN("year") '
            "FROM foo"
        ),
        (
            "SELECT "
            'MAX("year"), AVG("year"::double precision), COUNT(DISTINCT "name"), COUNT("name") '
            "FROM foo"
        ),
    ]

    assert (
        PostgreSQLProfileExtractor._build_profiling_query(
            dataset, column_statistics, SamplingConfig(), max_entities_per_query=5
        )
        == expected
    )


def test_build_profiling_query_with_sampling():
    dataset = init_dataset(name="foo", row_count=1000000000000)
    dataset.schema.fields = [
        SchemaField(field_path="id", field_name="id", nullable=False, subfields=None),
        SchemaField(
            field_path="price",
            field_name="price",
            nullable=True,
            precision=22.0,
            subfields=None,
        ),
    ]

    expected = [
        (
            'SELECT COUNT(1), COUNT(DISTINCT "id"), COUNT(DISTINCT "price"), '
            'COUNT("price"), MIN("price"), MAX("price"), AVG("price"::double precision) '
            "FROM foo WHERE random() < 0.01"
        )
    ]

    assert (
        PostgreSQLProfileExtractor._build_profiling_query(
            dataset,
            column_statistics,
            SamplingConfig(percentage=1, threshold=100000000),
        )
        == expected
    )


def test_parse_profiling_result():
    dataset = init_dataset(name="foo", row_count=1000)
    dataset.schema.fields = [
        SchemaField(field_path="id", field_name="id", nullable=False, subfields=None),
        SchemaField(
            field_path="price",
            field_name="price",
            nullable=True,
            precision=22.0,
            subfields=None,
        ),
        SchemaField(
            field_path="year",
            field_name="year",
            nullable=True,
            precision=32.0,
            subfields=None,
        ),
    ]
    dataset.field_statistics = DatasetFieldStatistics(field_statistics=[])

    results = [5, 5, 4, 0, 3, 8, 5, 2, 1, 2000, 2020, 2015]

    PostgreSQLProfileExtractor._parse_result(results, dataset, column_statistics)
    dataset.schema = None
    dataset.statistics = None

    assert dataset == Dataset(
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
                ),
                FieldStatistics(
                    average=2015.0,
                    distinct_value_count=2.0,
                    field_path="year",
                    max_value=2020.0,
                    min_value=2000.0,
                    nonnull_value_count=4.0,
                    null_value_count=1.0,
                ),
            ]
        ),
        logical_id=DatasetLogicalID(name="foo", platform=DataPlatform.POSTGRESQL),
    )
