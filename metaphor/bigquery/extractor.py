from concurrent.futures import ThreadPoolExecutor
from typing import (
    Any,
    Collection,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from metaphor.bigquery.logEvent import JobChangeEvent
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.query_history import chunk_query_logs
from metaphor.common.utils import md5_digest, start_of_day

try:
    import google.cloud.bigquery as bigquery
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.bigquery.utils import LogEntry, build_client, build_logging_client
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
    DatasetStructure,
    MaterializationType,
    QueriedDataset,
    QueryLog,
    SchemaField,
    SchemaType,
    SQLSchema,
)

logger = get_logger()


class BigQueryExtractor(BaseExtractor):
    """BigQuery metadata extractor"""

    BYTES_PER_MEGABYTES = 1024 * 1024

    @staticmethod
    def from_config_file(config_file: str) -> "BigQueryExtractor":
        return BigQueryExtractor(BigQueryRunConfig.from_yaml_file(config_file))

    def __init__(self, config: BigQueryRunConfig) -> None:
        super().__init__(config, "BigQuery metadata crawler", Platform.BIGQUERY)
        self._client = build_client(config)
        self._logging_client = build_logging_client(config)
        self._project_id = config.project_id
        self._job_project_id = config.job_project_id
        self._dataset_filter = config.filter.normalize()
        self._max_concurrency = config.max_concurrency
        self._query_log_lookback_days = config.query_log.lookback_days
        self._query_log_excluded_usernames = config.query_log.excluded_usernames
        self._query_log_exclude_service_accounts = (
            config.query_log.exclude_service_accounts
        )
        self._query_log_fetch_size = config.query_log.fetch_size

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

        logger.info("Fetching BigQueryAuditMetadata")
        query_logs = self._fetch_query_logs()

        entities: List[ENTITY_TYPES] = fetched_tables
        entities.extend(chunk_query_logs(query_logs))
        return entities

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
            name=dataset_normalized_name(
                db=project_id, schema=bq_table.dataset_id, table=bq_table.table_id
            ),
        )

        schema = BigQueryExtractor.parse_schema(bq_table)
        statistics = BigQueryExtractor._parse_statistics(bq_table)

        return Dataset(
            logical_id=dataset_id,
            schema=schema,
            statistics=statistics,
            structure=DatasetStructure(
                database=project_id, schema=bq_table.dataset_id, table=bq_table.table_id
            ),
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

    def _fetch_query_logs(self) -> List[QueryLog]:
        logs: List[Optional[QueryLog]] = []
        log_filter = self._build_job_change_filter()

        for entry in self._logging_client.list_entries(
            page_size=self._query_log_fetch_size, filter_=log_filter
        ):
            if JobChangeEvent.can_parse(entry):
                logs.append(self._parse_job_change_entry(entry))

            if len(logs) % 1000 == 0:
                logger.info(f"Fetched {len(logs)} audit logs")

        logger.info(f"Number of audit log entries fetched: {len(logs)}")

        return [log for log in logs if log is not None]

    def _parse_job_change_entry(self, entry: LogEntry) -> Optional[QueryLog]:
        job_change = JobChangeEvent.from_entry(entry)
        if job_change is None or job_change.query is None:
            return None

        if job_change.user_email in self._query_log_excluded_usernames:
            logger.debug(f"Skipped query issued by {job_change.user_email}")
            return None

        sources: List[QueriedDataset] = [
            QueriedDataset(database=d.project_id, schema=d.dataset_id, table=d.table_id)
            for d in job_change.source_tables
        ]
        target = job_change.destination_table
        target_datasets = (
            [
                QueriedDataset(
                    database=target.project_id,
                    schema=target.dataset_id,
                    table=target.table_id,
                )
            ]
            if target
            else None
        )

        elapsed_time = (
            (job_change.end_time - job_change.start_time).total_seconds()
            if job_change.start_time and job_change.end_time
            else None
        )

        return QueryLog(
            id=f"{DataPlatform.BIGQUERY.name}:{job_change.job_name}",
            query_id=job_change.job_name,
            platform=DataPlatform.BIGQUERY,
            start_time=job_change.start_time,
            duration=float(elapsed_time),
            email=job_change.user_email,
            rows_written=float(job_change.output_rows)
            if job_change.output_rows
            else None,
            bytes_read=float(job_change.input_bytes)
            if job_change.input_bytes
            else None,
            bytes_written=float(job_change.output_bytes)
            if job_change.output_bytes
            else None,
            sources=sources,
            targets=target_datasets,
            sql=job_change.query,
            sql_hash=md5_digest(job_change.query.encode("utf-8")),
        )

    def _build_job_change_filter(self) -> str:
        start_time = start_of_day(self._query_log_lookback_days).isoformat()
        end_time = start_of_day().isoformat()

        # Filter for service account
        service_account_filter = (
            "NOT protoPayload.authenticationInfo.principalEmail:gserviceaccount.com AND"
            if self._query_log_exclude_service_accounts
            else ""
        )

        # See https://cloud.google.com/logging/docs/view/logging-query-language for query syntax
        return f"""
        resource.type="bigquery_project" AND
        protoPayload.serviceName="bigquery.googleapis.com" AND
        protoPayload.metadata.jobChange.after="DONE" AND
        NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult.code:* AND
        protoPayload.metadata.jobChange.job.jobConfig.type=("COPY" OR "QUERY") AND
        {service_account_filter}
        timestamp>="{start_time}" AND
        timestamp<"{end_time}"
        """
