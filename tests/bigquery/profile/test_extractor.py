from google.cloud.bigquery import DatasetReference, TableReference
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    DatasetSchema,
    EntityType,
    FieldStatistics,
    SchemaField,
)

from metaphor.bigquery.profile.extractor import BigQueryProfileExtractor


def test_build_profiling_query():
    schema = DatasetSchema(
        fields=[
            SchemaField(field_path="id", native_type="STRING", nullable=False),
            SchemaField(field_path="price", native_type="INT", nullable=True),
            SchemaField(field_path="json", native_type="RECORD", nullable=True),
        ]
    )

    table = TableReference(DatasetReference("project", "dataset_id"), "table_id")

    expected = (
        "SELECT COUNT(1), COUNT(DISTINCT id), COUNT(DISTINCT price), "
        "countif(price is NULL), MIN(price), MAX(price), AVG(price), "
        "countif(json is NULL) "
        "FROM `project.dataset_id.table_id`"
    )

    assert (
        BigQueryProfileExtractor._build_profiling_query(schema, table, 1000, 50)
        == expected
    )


def test_build_profiling_query_with_sampling():
    schema = DatasetSchema(
        fields=[
            SchemaField(field_path="id", native_type="STRING", nullable=False),
            SchemaField(field_path="price", native_type="INT", nullable=True),
        ]
    )

    table = TableReference(DatasetReference("project", "dataset_id"), "table_id")

    expected = (
        "SELECT COUNT(1), COUNT(DISTINCT id), COUNT(DISTINCT price), "
        "countif(price is NULL), MIN(price), MAX(price), AVG(price) "
        "FROM `project.dataset_id.table_id` TABLESAMPLE SYSTEM (50 PERCENT)"
    )

    assert (
        BigQueryProfileExtractor._build_profiling_query(schema, table, 1000000, 50)
        == expected
    )


def test_parse_profiling_result():
    schema = DatasetSchema(
        fields=[
            SchemaField(field_path="id", native_type="STRING", nullable=False),
            SchemaField(field_path="price", native_type="INT", nullable=True),
            SchemaField(field_path="json", native_type="RECORD", nullable=True),
        ]
    )
    results = (5, 5, 4, 0, 3, 8, 5, 2)
    dataset = BigQueryProfileExtractor._init_dataset(full_name="foo")

    BigQueryProfileExtractor._parse_result(results, schema, dataset)

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
                ),
                FieldStatistics(
                    field_path="json",
                    nonnull_value_count=3.0,
                    null_value_count=2.0,
                ),
            ]
        ),
        logical_id=DatasetLogicalID(name="foo", platform=DataPlatform.BIGQUERY),
    )
