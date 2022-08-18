from concurrent.futures import ThreadPoolExecutor
from typing import Any, Collection, Dict, Iterable, List, Mapping, Sequence, Union

try:
    import google.cloud.bigquery as bigquery
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.bigquery.utils import build_client
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import FieldDataType, build_field_path
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
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

logger = get_logger(__name__)


class BigQueryExtractor(BaseExtractor):
    """BigQuery metadata extractor"""

    BYTES_PER_MEGABYTES = 1024 * 1024

    @staticmethod
    def from_config_file(config_file: str) -> "BigQueryExtractor":
        return BigQueryExtractor(BigQueryRunConfig.from_yaml_file(config_file))

    def __init__(self, config: BigQueryRunConfig) -> None:
        super().__init__(config, "BigQuery metadata crawler", Platform.BIGQUERY)
        self._client = build_client(config)
        self._project_id = config.project_id
        self._job_project_id = config.job_project_id
        self._dataset_filter = config.filter.normalize()
        self._max_concurrency = config.max_concurrency

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from BigQuery project {self._project_id}")

        fetched_tables: List[Dataset] = []
        for dataset_ref in BigQueryExtractor._list_datasets_with_filter(
            self._client, self._dataset_filter
        ):
            logger.info(f"Fetching tables for {dataset_ref}")

            with ThreadPoolExecutor(max_workers=self._max_concurrency) as executor:

                def get_table(table: bigquery.TableReference) -> Dataset:
                    logger.info(f"Getting table {table.table_id}")
                    bq_table = self._client.get_table(table)
                    return self._parse_table(self._client.project, bq_table)

                # map of table name to Dataset
                tables: Dict[str, Dataset] = {
                    d.logical_id.name.split(".")[-1]: d
                    for d in executor.map(
                        get_table,
                        BigQueryExtractor._list_tables_with_filter(
                            dataset_ref, self._client, self._dataset_filter
                        ),
                    )
                }

            logger.info(f"Getting table DDL for {dataset_ref}")
            table_ddl = self._client.query(
                f"select table_name, ddl from `{dataset_ref.project}.{dataset_ref.dataset_id}.INFORMATION_SCHEMA.TABLES`",
                project=self._job_project_id,
            ).result()

            for table_name, ddl in table_ddl:
                table = tables.get(str(table_name).lower())
                if table is None:
                    logger.error(f"table {table_name} not found for DDL")
                    continue
                table.schema.sql_schema.table_schema = ddl

            fetched_tables.extend(tables.values())

        return fetched_tables

    @staticmethod
    def _list_datasets_with_filter(
        client: bigquery.Client, dataset_filter: DatasetFilter
    ) -> Iterable[bigquery.DatasetReference]:
        for bq_dataset in client.list_datasets():
            if not dataset_filter.include_schema(client.project, bq_dataset.dataset_id):
                logger.info(f"Skipped dataset {bq_dataset.dataset_id}")
                continue

            dataset_ref = bigquery.DatasetReference(
                client.project, bq_dataset.dataset_id
            )
            yield dataset_ref

    @staticmethod
    def _list_tables_with_filter(
        dataset_ref: bigquery.DatasetReference,
        client: bigquery.Client,
        dataset_filter: DatasetFilter,
    ) -> Iterable[bigquery.TableReference]:
        for bq_table in client.list_tables(dataset_ref.dataset_id):
            table_ref = dataset_ref.table(bq_table.table_id)
            if not dataset_filter.include_table(
                client.project, dataset_ref.dataset_id, bq_table.table_id
            ):
                logger.info(f"Skipped table: {table_ref}")
                continue
            yield table_ref

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

        schema.fields = BigQueryExtractor._parse_fields(bq_table.schema, "")

        return schema

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.schema.SchemaField.html#google.cloud.bigquery.schema.SchemaField
    @staticmethod
    def _parse_fields(
        schema: Sequence[Union[bigquery.schema.SchemaField, Mapping[str, Any]]],
        parent_field_path: str,
    ) -> List[SchemaField]:

        fields: List[SchemaField] = []
        for field in schema:

            # There's no documentation on how to handle the Mapping[str, Any] type.
            # Actual API also doesn't seem to return this type: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
            if not isinstance(field, bigquery.schema.SchemaField):
                raise ValueError(f"Field type not supported: {field}")

            # mode REPEATED means ARRAY
            native_type = (
                f"ARRAY<{field.field_type}>"
                if field.mode == "REPEATED"
                else field.field_type
            )

            # See https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.type
            field_type = (
                FieldDataType.ARRAY
                if field.mode == "REPEATED"
                else FieldDataType.RECORD
                if field.field_type in ("RECORD", "STRUCT")
                else FieldDataType.PRIMITIVE
            )

            field_path = build_field_path(parent_field_path, field.name, field_type)

            subfields = None
            if field.fields is not None and len(field.fields) > 0:
                subfields = BigQueryExtractor._parse_fields(field.fields, field_path)

            fields.append(
                SchemaField(
                    field_path=field_path,
                    field_name=field.name,
                    description=field.description,
                    native_type=native_type,
                    nullable=field.is_nullable,
                    subfields=subfields,
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
