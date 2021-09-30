import json
import logging
import re
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, List, Mapping, Optional, Sequence, Union

from serde import deserialize
from smart_open import open

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
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
    MetadataChangeEvent,
    SchemaField,
    SchemaType,
    SQLSchema,
)

from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor, RunConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@deserialize
@dataclass
class BigQueryRunConfig(RunConfig):
    # Path to service account's JSON key file
    key_path: str

    # Project ID to use. Use the service account's default project if not set
    project_id: Optional[str] = None

    # Filters for dataset names (any match will be included)
    dataset_filters: List[str] = dataclass_field(default_factory=lambda: [r".*"])


def build_client(config: BigQueryRunConfig):
    with open(config.key_path) as fin:
        credentials = service_account.Credentials.from_service_account_info(
            json.load(fin),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        return bigquery.Client(
            credentials=credentials,
            project=config.project_id if config.project_id else credentials.project_id,
        )


class BigQueryExtractor(BaseExtractor):
    """BigQuery metadata extractor"""

    @staticmethod
    def config_class():
        return BigQueryRunConfig

    BYTES_PER_MEGABYTES = 1024 * 1024

    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, BigQueryExtractor.config_class())

        logger.info("Fetching metadata from BigQuery")

        client = build_client(config)

        dataset_patterns = [re.compile(f) for f in config.dataset_filters]

        datasets: List[Dataset] = []
        for bq_dataset in client.list_datasets():
            if self._skip_dataset(bq_dataset.dataset_id, dataset_patterns):
                logger.info(f"Skipped dataset {bq_dataset.dataset_id}")
                continue

            dataset_ref = bigquery.DatasetReference(
                client.project, bq_dataset.dataset_id
            )

            logger.info(f"Found dataset {dataset_ref}")

            for bq_table in client.list_tables(bq_dataset.dataset_id):
                table_ref = dataset_ref.table(bq_table.table_id)
                logger.info(f"Found table {table_ref}")

                bq_table = client.get_table(table_ref)
                datasets.append(self._parse_table(client.project, bq_table))

        return [EventUtil.build_dataset_event(d) for d in datasets]

    # True if dataset_id should be skipped as it doesn't match any of the patterns
    def _skip_dataset(self, dataset_id, patterns):
        for pattern in patterns:
            if re.match(pattern, dataset_id) is not None:
                return False

        return True

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html#google.cloud.bigquery.table.Table
    def _parse_table(self, project_id, bq_table: bigquery.table.Table) -> Dataset:
        dataset_id = DatasetLogicalID(
            platform=DataPlatform.BIGQUERY,
            name=f"{project_id}.{bq_table.dataset_id}.{bq_table.table_id}",
        )

        schema = self._parse_schema(bq_table)
        statistics = self._parse_statistics(bq_table)

        return Dataset(
            logical_id=dataset_id,
            schema=schema,
            statistics=statistics,
        )

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html#google.cloud.bigquery.table.Table
    def _parse_schema(self, bq_table: bigquery.table.Table) -> DatasetSchema:
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

        schema.fields = self._parse_fields(bq_table.schema)

        return schema

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.schema.SchemaField.html#google.cloud.bigquery.schema.SchemaField
    def _parse_fields(
        self, schema: Sequence[Union[bigquery.schema.SchemaField, Mapping[str, Any]]]
    ) -> List[SchemaField]:

        fields: List[SchemaField] = []
        for field in schema:

            # There's no documentation on how to handle the Mapping[str, Any] type.
            # Actual API also doesn't seem to return this type: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
            if not isinstance(field, bigquery.schema.SchemaField):
                raise ValueError(f"Field type not supported: {field}")

            fields.append(
                SchemaField(
                    field_path=field.name,
                    description=field.description,
                    native_type=field.field_type,
                    nullable=field.is_nullable,
                )
            )

        return fields

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html#google.cloud.bigquery.table.Table
    def _parse_statistics(self, bq_table: bigquery.table.Table) -> DatasetStatistics:
        return DatasetStatistics(
            data_size=float(bq_table.num_bytes / BigQueryExtractor.BYTES_PER_MEGABYTES),
            record_count=float(bq_table.num_rows),
            last_updated=bq_table.modified,
        )
