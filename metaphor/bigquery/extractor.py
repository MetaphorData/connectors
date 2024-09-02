import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import (
    Any,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from metaphor.common.sql.process_query.process_query import process_query

try:
    import google.cloud.bigquery as bigquery
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.bigquery.logEvent import JobChangeEvent
from metaphor.bigquery.utils import (
    BigQueryResource,
    LogEntry,
    build_client,
    build_logging_client,
    get_credentials,
)
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import FieldDataType, build_field_path
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.common.tag_matcher import tag_datasets
from metaphor.common.utils import md5_digest, safe_float, start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    LogType,
    MaterializationType,
    QueriedDataset,
    QueryLog,
    SchemaField,
    SchemaType,
    SourceInfo,
    SQLSchema,
)

logger = get_logger()


class BigQueryExtractor(BaseExtractor):
    """BigQuery metadata extractor"""

    _description = "BigQuery metadata crawler"
    _platform = Platform.BIGQUERY

    @staticmethod
    def from_config_file(config_file: str) -> "BigQueryExtractor":
        return BigQueryExtractor(BigQueryRunConfig.from_yaml_file(config_file))

    def __init__(self, config: BigQueryRunConfig) -> None:
        super().__init__(config)
        self._config = config
        self._credentials = get_credentials(config)
        self._project_ids = config.project_ids
        self._job_project_id = config.job_project_id or self._credentials.project_id
        self._dataset_filter = config.filter.normalize()
        self._max_concurrency = config.max_concurrency
        self._tag_matchers = config.tag_matchers
        self._query_log_lookback_days = config.query_log.lookback_days
        self._query_log_excluded_usernames = config.query_log.excluded_usernames
        self._query_log_exclude_service_accounts = (
            config.query_log.exclude_service_accounts
        )
        self._query_log_fetch_size = config.query_log.fetch_size
        self._fetch_job_query_if_truncated = (
            config.query_log.fetch_job_query_if_truncated
        )

        self._datasets: List[Dataset] = []

    async def extract(self) -> Collection[ENTITY_TYPES]:
        for project_id in self._project_ids:
            self._extract_project(project_id)

        return self._datasets

    def collect_query_logs(self) -> Iterator[QueryLog]:
        for project_id in self._project_ids:
            yield from self._fetch_query_logs(project_id)

    def _extract_project(self, project_id: str) -> None:
        logger.info(f"Fetching metadata from BigQuery project {project_id}")

        client = build_client(project_id, self._credentials)

        fetched_tables: List[Dataset] = []
        for dataset_ref in BigQueryExtractor._list_datasets_with_filter(
            client, self._dataset_filter
        ):
            logger.info(f"Fetching tables for {dataset_ref}")

            with ThreadPoolExecutor(max_workers=self._max_concurrency) as executor:

                def get_table(table: bigquery.TableReference) -> Dataset:
                    logger.info(f"Getting table {table.table_id}")
                    bq_table = client.get_table(table)
                    return self._parse_table(client.project, bq_table)

                # map of table name to Dataset
                tables: Dict[str, Dataset] = {
                    d.logical_id.name.split(".")[-1]: d
                    for d in executor.map(
                        get_table,
                        BigQueryExtractor._list_tables_with_filter(
                            dataset_ref, client, self._dataset_filter
                        ),
                    )
                }

            logger.info(f"Getting table DDL for {dataset_ref}")
            table_ddl = client.query(
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

        tag_datasets(fetched_tables, self._tag_matchers)

        self._datasets.extend(fetched_tables)

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
        statistics = to_dataset_statistics(bq_table.num_rows, bq_table.num_bytes)

        return Dataset(
            logical_id=dataset_id,
            schema=schema,
            statistics=statistics,
            structure=DatasetStructure(
                database=project_id, schema=bq_table.dataset_id, table=bq_table.table_id
            ),
            source_info=SourceInfo(
                created_at_source=bq_table.created, last_updated=bq_table.modified
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
        elif bq_table.table_type == "SNAPSHOT":

            schema.sql_schema = SQLSchema(
                materialization=MaterializationType.SNAPSHOT,
                snapshot_time=BigQueryExtractor._get_snapshot_time(bq_table),
            )
        else:
            raise ValueError(f"Unexpected table type {bq_table.table_type}")

        schema.fields = BigQueryExtractor._parse_fields(bq_table.schema, "")

        return schema

    @staticmethod
    def _get_snapshot_time(bq_table: bigquery.table.Table) -> Optional[datetime]:
        # bigquery client fails to parse snapshot time sometimes
        # See https://github.com/googleapis/python-bigquery/issues/1986
        try:
            if bq_table.snapshot_definition:
                return bq_table.snapshot_definition.snapshot_time
        except ValueError:
            return None

        return None

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
                else (
                    FieldDataType.RECORD
                    if field.field_type in ("RECORD", "STRUCT")
                    else FieldDataType.PRIMITIVE
                )
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

    def _fetch_query_logs(self, project_id: str) -> Iterator[QueryLog]:
        log_filter = self._build_job_change_filter()

        client = build_client(project_id, self._credentials)
        logging_client = build_logging_client(project_id, self._credentials)
        fetched_logs = 0
        for entry in logging_client.list_entries(
            page_size=self._query_log_fetch_size, filter_=log_filter
        ):
            if JobChangeEvent.can_parse(entry):
                log = self._parse_job_change_entry(entry, client)
                if log:
                    fetched_logs += 1
                    yield log

        logger.info(f"Number of audit log entries fetched: {fetched_logs}")

    @staticmethod
    def _fetch_job_query(client: bigquery.Client, job_name: str) -> Optional[str]:
        logger.info(f"Query {job_name}")
        match = re.match(r"^projects/([^/]+)/jobs/([^/]+)$", job_name)
        if match:
            project = match.group(1)
            job_id = match.group(2)

            try:
                job = client.get_job(job_id, project)
            except Exception as e:
                logger.warning(f"Failed to get job information: {e}")
                return None

            if isinstance(job, bigquery.QueryJob):
                return job.query

        return None

    def _parse_job_change_entry(
        self, entry: LogEntry, client: bigquery.Client
    ) -> Optional[QueryLog]:
        job_change = JobChangeEvent.from_entry(entry)
        if job_change is None or job_change.query is None:
            return None

        if job_change.user_email in self._query_log_excluded_usernames:
            logger.debug(f"Skipped query issued by {job_change.user_email}")
            return None

        sources: List[QueriedDataset] = [
            self._convert_resource_to_queried_dataset(d)
            for d in job_change.source_tables
        ]
        target = job_change.destination_table
        target_datasets = (
            [self._convert_resource_to_queried_dataset(target)] if target else None
        )

        default_database, default_schema = None, None
        if job_change.default_dataset and job_change.default_dataset.count(".") == 1:
            default_database, default_schema = job_change.default_dataset.split(".")

        query = job_change.query
        # if query SQL is truncated, fetch full SQL from job API
        if (
            job_change.job_type == "QUERY"
            and job_change.query_truncated
            and self._fetch_job_query_if_truncated
        ):
            query = self._fetch_job_query(client, job_change.job_name) or query

        elapsed_time = (
            (job_change.end_time - job_change.start_time).total_seconds()
            if job_change.start_time and job_change.end_time
            else None
        )

        # Assume service accounts always end in ".gserviceaccount.com"
        # https://cloud.google.com/compute/docs/access/service-accounts
        is_service_account = job_change.user_email.endswith(".gserviceaccount.com")

        sql: Optional[str] = query
        if self._config.query_log.process_query.should_process:
            sql = process_query(
                query,
                DataPlatform.BIGQUERY,
                self._config.query_log.process_query,
                job_change.job_name,
            )

        if sql:
            return QueryLog(
                id=f"{DataPlatform.BIGQUERY.name}:{job_change.job_name}",
                query_id=job_change.job_name,
                platform=DataPlatform.BIGQUERY,
                start_time=job_change.start_time,
                duration=safe_float(elapsed_time),
                user_id=job_change.user_email if is_service_account else None,
                email=job_change.user_email if not is_service_account else None,
                rows_written=(
                    float(job_change.output_rows) if job_change.output_rows else None
                ),
                bytes_read=(
                    float(job_change.input_bytes) if job_change.input_bytes else None
                ),
                bytes_written=(
                    float(job_change.output_bytes) if job_change.output_bytes else None
                ),
                sources=sources,
                targets=target_datasets,
                default_database=default_database,
                default_schema=default_schema,
                type=(
                    self._map_query_type(job_change.statementType)
                    if job_change.statementType
                    else None
                ),
                sql=sql,
                sql_hash=md5_digest(sql.encode("utf-8")),
            )

        return None

    # https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.QueryStatementType
    _query_type_map = {
        "SELECT": LogType.SELECT,
        "INSERT": LogType.INSERT,
        "UPDATE": LogType.UPDATE,
        "DELETE": LogType.DELETE,
        "MERGE": LogType.MERGE,
        "CREATE_TABLE": LogType.CREATE_TABLE,
        "CREATE_TABLE_AS_SELECT": LogType.CREATE_TABLE,
        "CREATE_SNAPSHOT_TABLE": LogType.CREATE_TABLE,
        "CREATE_VIEW": LogType.CREATE_VIEW,
        "CREATE_MATERIALIZED_VIEW": LogType.CREATE_VIEW,
        "DROP_TABLE": LogType.DROP_TABLE,
        "DROP_EXTERNAL_TABLE": LogType.DROP_TABLE,
        "DROP_SNAPSHOT_TABLE": LogType.DROP_TABLE,
        "DROP_VIEW": LogType.DROP_VIEW,
        "DROP_MATERIALIZED_VIEW": LogType.DROP_VIEW,
        "ALTER_TABLE": LogType.ALTER_TABLE,
        "ALTER_VIEW": LogType.ALTER_VIEW,
        "ALTER_MATERIALIZED_VIEW": LogType.ALTER_VIEW,
        "TRUNCATE_TABLE": LogType.TRUNCATE,
        "EXPORT_DATA": LogType.EXPORT,
    }

    @staticmethod
    def _convert_resource_to_queried_dataset(
        resource: BigQueryResource,
    ) -> QueriedDataset:
        dataset_name = dataset_normalized_name(
            resource.project_id, resource.dataset_id, resource.table_id
        )
        dataset_id = str(to_dataset_entity_id(dataset_name, DataPlatform.BIGQUERY))
        return QueriedDataset(
            id=dataset_id,
            database=resource.project_id,
            schema=resource.dataset_id,
            table=resource.table_id,
        )

    @staticmethod
    def _map_query_type(query_type: str) -> LogType:
        return BigQueryExtractor._query_type_map.get(query_type.upper(), LogType.OTHER)

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
