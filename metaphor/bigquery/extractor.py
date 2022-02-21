import logging
from typing import Any, Collection, List, Mapping, Sequence, Union

try:
    import google.cloud.bigquery as bigquery
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStatistics,
    MaterializationType,
    SchemaField,
    SchemaType,
    SQLSchema,
)

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.bigquery.utils import build_client
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BigQueryExtractor(BaseExtractor):
    """BigQuery metadata extractor"""

    @staticmethod
    def config_class():
        return BigQueryRunConfig

    BYTES_PER_MEGABYTES = 1024 * 1024

    async def extract(self, config: BigQueryRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, BigQueryExtractor.config_class())

        logger.info("Fetching metadata from BigQuery")

        client = build_client(config)

        dataset_filter = config.filter.normalize()

        datasets: List[Dataset] = []
        for bq_dataset in client.list_datasets():
            if not dataset_filter.include_schema(
                config.project_id, bq_dataset.dataset_id
            ):
                logger.info(f"Skipped dataset {bq_dataset.dataset_id}")
                continue

            dataset_ref = bigquery.DatasetReference(
                client.project, bq_dataset.dataset_id
            )

            logger.info(f"Found dataset {dataset_ref}")

            for bq_table in client.list_tables(bq_dataset.dataset_id):
                table_ref = dataset_ref.table(bq_table.table_id)
                if not dataset_filter.include_table(
                    config.project_id, bq_dataset.dataset_id, bq_table.table_id
                ):
                    logger.info(f"Skipped table: {table_ref}")
                    continue
                logger.info(f"Found table {table_ref}")

                bq_table = client.get_table(table_ref)
                datasets.append(self._parse_table(client.project, bq_table))

        return datasets

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html#google.cloud.bigquery.table.Table
    @staticmethod
    def _parse_table(project_id, bq_table: bigquery.table.Table) -> Dataset:
        dataset_id = DatasetLogicalID(
            platform=DataPlatform.BIGQUERY,
            name=f"{project_id}.{bq_table.dataset_id}.{bq_table.table_id}",
        )

        schema = BigQueryExtractor.parse_schema(bq_table)
        statistics = BigQueryExtractor._parse_statistics(bq_table)

        return Dataset(
            logical_id=dataset_id,
            schema=schema,
            statistics=statistics,
        )

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html#google.cloud.bigquery.table.Table
    @staticmethod
    def parse_schema(bq_table: bigquery.table.Table) -> DatasetSchema:
        schema = DatasetSchema(
            description=bq_table.description, schema_type=SchemaType.SQL
        )

        if bq_table.table_type == "TABLE":
            schema.sql_schema = SQLSchema(materialization=MaterializationType.TABLE)
        elif bq_table.table_type == "EXTERNAL":
            schema.sql_schema = SQLSchema(materialization=MaterializationType.EXTERNAL)
        elif bq_table.table_type == "VIEW":
            schema.sql_schema = SQLSchema(
                materialization=MaterializationType.VIEW,
                table_schema=bq_table.view_query,
            )
        elif bq_table.table_type == "MATERIALIZED_VIEW":
            schema.sql_schema = SQLSchema(
                materialization=MaterializationType.MATERIALIZED_VIEW,
                table_schema=bq_table.mview_query,
            )
        else:
            raise ValueError(f"Unexpected table type {bq_table.table_type}")

        schema.fields = BigQueryExtractor._parse_fields(bq_table.schema)

        return schema

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.schema.SchemaField.html#google.cloud.bigquery.schema.SchemaField
    @staticmethod
    def _parse_fields(
        schema: Sequence[Union[bigquery.schema.SchemaField, Mapping[str, Any]]]
    ) -> List[SchemaField]:

        fields: List[SchemaField] = []
        for field in schema:

            # There's no documentation on how to handle the Mapping[str, Any] type.
            # Actual API also doesn't seem to return this type: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
            if not isinstance(field, bigquery.schema.SchemaField):
                raise ValueError(f"Field type not supported: {field}")

            # mode REPEATED means ARRAY
            field_type = (
                f"ARRAY<{field.field_type}>"
                if field.mode == "REPEATED"
                else field.field_type
            )

            fields.append(
                SchemaField(
                    field_path=field.name,
                    description=field.description,
                    native_type=field_type,
                    nullable=field.is_nullable,
                )
            )

        return fields

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html#google.cloud.bigquery.table.Table
    @staticmethod
    def _parse_statistics(bq_table: bigquery.table.Table) -> DatasetStatistics:
        return DatasetStatistics(
            data_size=float(bq_table.num_bytes / BigQueryExtractor.BYTES_PER_MEGABYTES),
            record_count=float(bq_table.num_rows),
            last_updated=bq_table.modified,
        )
