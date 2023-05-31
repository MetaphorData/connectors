from google.cloud.bigquery import DatasetReference, TableReference

from metaphor.bigquery.profile.extractor import BigQueryProfileExtractor
from metaphor.common.column_statistics import ColumnStatistics
from metaphor.common.sampling import SamplingConfig
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    DatasetSchema,
    FieldStatistics,
    SchemaField,
)


def test_build_profiling_query_default():
    schema = DatasetSchema(
        fields=[
            SchemaField(
                field_path="id",
                field_name="id",
                native_type="STRING",
                nullable=False,
                subfields=None,
            ),
            SchemaField(
                field_path="price",
                field_name="price",
                native_type="INT",
                nullable=True,
                subfields=None,
            ),
            SchemaField(
                field_path="json",
                field_name="json",
                native_type="RECORD",
                nullable=True,
                subfields=None,
            ),
        ]
    )

    table = TableReference(DatasetReference("project", "dataset_id"), "table_id")

    column_statistics = ColumnStatistics()
    sampling_config = SamplingConfig(percentage=50, threshold=100000)

    expected = (
        "SELECT COUNT(1), "
        "COUNTIF(`id` is NULL), "
        "COUNTIF(`price` is NULL), MIN(`price`), MAX(`price`), "
        "COUNTIF(`json` is NULL) "
        "FROM `project.dataset_id.table_id`"
    )

    assert (
        BigQueryProfileExtractor._build_profiling_query(
            schema, table, 1000, column_statistics, sampling_config
        )
        == expected
    )


def test_build_profiling_query_full():
    schema = DatasetSchema(
        fields=[
            SchemaField(
                field_path="id",
                field_name="id",
                native_type="STRING",
                nullable=False,
                subfields=None,
            ),
            SchemaField(
                field_path="price",
                field_name="price",
                native_type="INT",
                nullable=True,
                subfields=None,
            ),
            SchemaField(
                field_path="json",
                field_name="json",
                native_type="RECORD",
                nullable=True,
                subfields=None,
            ),
        ]
    )

    table = TableReference(DatasetReference("project", "dataset_id"), "table_id")

    column_statistics = ColumnStatistics(
        null_count=True,
        unique_count=True,
        min_value=True,
        max_value=True,
        avg_value=True,
        std_dev=True,
    )
    sampling_config = SamplingConfig(percentage=50, threshold=100000)

    expected = (
        "SELECT COUNT(1), "
        "COUNT(DISTINCT `id`), COUNTIF(`id` is NULL), "
        "COUNT(DISTINCT `price`), COUNTIF(`price` is NULL), MIN(`price`), MAX(`price`), AVG(`price`), STDDEV(`price`), "
        "COUNTIF(`json` is NULL) "
        "FROM `project.dataset_id.table_id`"
    )

    assert (
        BigQueryProfileExtractor._build_profiling_query(
            schema, table, 1000, column_statistics, sampling_config
        )
        == expected
    )


def test_build_profiling_query_with_sampling():
    schema = DatasetSchema(
        fields=[
            SchemaField(
                field_path="id",
                field_name="id",
                native_type="STRING",
                nullable=False,
                subfields=None,
            ),
            SchemaField(
                field_path="price",
                field_name="price",
                native_type="INT",
                nullable=True,
                subfields=None,
            ),
        ]
    )

    table = TableReference(DatasetReference("project", "dataset_id"), "table_id")

    column_statistics = ColumnStatistics()
    sampling_config = SamplingConfig(percentage=50, threshold=100000)

    expected = (
        "SELECT COUNT(1), "
        "COUNTIF(`id` is NULL), "
        "COUNTIF(`price` is NULL), MIN(`price`), MAX(`price`) "
        "FROM `project.dataset_id.table_id` "
        "TABLESAMPLE SYSTEM (50 PERCENT)"
    )

    assert (
        BigQueryProfileExtractor._build_profiling_query(
            schema, table, 1000000, column_statistics, sampling_config
        )
        == expected
    )


def test_parse_profiling_result_default():
    schema = DatasetSchema(
        fields=[
            SchemaField(
                field_path="id",
                field_name="id",
                native_type="STRING",
                nullable=False,
                subfields=None,
            ),
            SchemaField(
                field_path="price",
                field_name="price",
                native_type="INT",
                nullable=True,
                subfields=None,
            ),
            SchemaField(
                field_path="json",
                field_name="json",
                native_type="RECORD",
                nullable=True,
                subfields=None,
            ),
        ]
    )
    results = [
        # row count
        5,
        # id
        2,
        # price
        0,
        1,
        2,
        # json
        5,
    ]
    dataset = BigQueryProfileExtractor._init_dataset(full_name="foo")

    column_statistics = ColumnStatistics()

    BigQueryProfileExtractor._parse_result(results, schema, dataset, column_statistics)

    assert dataset == Dataset(
        field_statistics=DatasetFieldStatistics(
            field_statistics=[
                FieldStatistics(
                    field_path="id",
                    nonnull_value_count=3.0,
                    null_value_count=2.0,
                ),
                FieldStatistics(
                    field_path="price",
                    max_value=2.0,
                    min_value=1.0,
                    nonnull_value_count=5.0,
                    null_value_count=0.0,
                ),
                FieldStatistics(
                    field_path="json",
                    nonnull_value_count=0.0,
                    null_value_count=5.0,
                ),
            ]
        ),
        logical_id=DatasetLogicalID(name="foo", platform=DataPlatform.BIGQUERY),
    )
