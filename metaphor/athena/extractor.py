from typing import Collection, Dict, Iterator, List

import boto3

from metaphor.athena.config import AthenaRunConfig, AwsCredentials
from metaphor.athena.models import (
    BatchGetQueryExecutionResponse,
    TableMetadata,
    TableTypeEnum,
)
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.sql.query_log import PartialQueryLog, process_and_init_query_log
from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.common.utils import chunks, start_of_day, to_utc_time
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    MaterializationType,
    QueryLog,
    SchemaField,
    SchemaType,
    SourceInfo,
    SQLSchema,
)

logger = get_logger()


def create_athena_client(aws: AwsCredentials) -> boto3.client:
    return aws.get_session().client("athena")


SUPPORTED_CATALOG_TYPE = ("GLUE", "HIVE")


class AthenaExtractor(BaseExtractor):
    """Athena metadata extractor"""

    _description = "Athena metadata crawler"
    _platform = Platform.ATHENA

    @staticmethod
    def from_config_file(config_file: str) -> "AthenaExtractor":
        return AthenaExtractor(AthenaRunConfig.from_yaml_file(config_file))

    def __init__(self, config: AthenaRunConfig) -> None:
        super().__init__(config)
        self._datasets: Dict[str, Dataset] = {}
        self._aws_config = config.aws
        self._filter = config.filter.normalize()
        self._query_log_config = config.query_log

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Athena")

        self._client = create_athena_client(self._aws_config)

        for catalog in self._get_catalogs():
            if not self._filter.include_database(database_name=catalog):
                logger.info(f"Skipping catalog: {catalog}")
                continue

            databases = self._get_databases(catalog)
            for database in databases:
                if not self._filter.include_schema(database=catalog, schema=database):
                    logger.info(f"Skipping database: {catalog}.{database}")
                    continue

                self._extract_tables(catalog, database)

        return self._datasets.values()

    def collect_query_logs(self) -> Iterator[QueryLog]:
        # If empty, don't call list_query_executions with WorkGroup
        work_groups = self._query_log_config.work_groups or [""]

        if self._query_log_config.lookback_days > 0:
            for workgroup in work_groups:
                params = {}
                if workgroup:
                    params["WorkGroup"] = workgroup

                for page in self._paginate_and_dump_response(
                    "list_query_executions", **params
                ):
                    ids = page["QueryExecutionIds"]
                    yield from self._batch_get_queries(ids)

    def _get_catalogs(self):
        database_names = []
        for page in self._paginate_and_dump_response("list_data_catalogs"):
            for item in page["DataCatalogsSummary"]:
                if item["Type"] not in SUPPORTED_CATALOG_TYPE:
                    continue
                database_names.append(item["CatalogName"])

        return database_names

    def _get_databases(self, catalog: str):
        database_names = []
        for page in self._paginate_and_dump_response(
            "list_databases", CatalogName=catalog
        ):
            for database in page["DatabaseList"]:
                database_names.append(database["Name"])

        return database_names

    def _extract_tables(self, catalog: str, database: str):
        for page in self._paginate_and_dump_response(
            "list_table_metadata", CatalogName=catalog, DatabaseName=database
        ):
            for table_metadata in page["TableMetadataList"]:
                self._init_dataset(catalog, database, TableMetadata(**table_metadata))

    def _paginate_and_dump_response(self, api_endpoint: str, **paginator_args):
        paginator = self._client.get_paginator(api_endpoint)
        paginator_response = paginator.paginate(**paginator_args)

        for page in paginator_response:
            request_id = page["ResponseMetadata"]["RequestId"]
            json_dump_to_debug_file(page, f"{api_endpoint}_{request_id}.json")
            yield page

    def _init_dataset(self, catalog: str, database: str, table_metadata: TableMetadata):
        table = table_metadata.Name

        if not self._filter.include_table(catalog, database, table):
            logger.info(f"Skipping table: {catalog}.{database}.{table}")
            return

        name = dataset_normalized_name(db=catalog, schema=database, table=table)

        table_type = (
            TableTypeEnum(table_metadata.TableType)
            if table_metadata.TableType
            and table_metadata.TableType in (enum.value for enum in TableTypeEnum)
            else None
        )

        dataset = Dataset(
            logical_id=DatasetLogicalID(
                name=name,
                platform=DataPlatform.ATHENA,
            ),
            source_info=SourceInfo(
                created_at_source=table_metadata.CreateTime,
            ),
            schema=(
                DatasetSchema(
                    fields=[
                        SchemaField(
                            description=column.Comment,
                            field_name=column.Name,
                            field_path=column.Name.lower(),
                            native_type=column.Type,
                        )
                        for column in table_metadata.Columns
                    ],
                    schema_type=SchemaType.SQL,
                    sql_schema=SQLSchema(
                        materialization=(
                            MaterializationType.TABLE
                            if table_type == TableTypeEnum.EXTERNAL_TABLE
                            else (
                                MaterializationType.VIEW
                                if table_type == TableTypeEnum.VIRTUAL_VIEW
                                else None
                            )
                        )
                    ),
                )
                if table_metadata.Columns
                else None
            ),
            structure=DatasetStructure(
                database=catalog,
                schema=database,
                table=table_metadata.Name,
            ),
        )

        self._datasets[name] = dataset

    def _batch_get_queries(self, query_execution_ids: List[str]) -> List[QueryLog]:
        query_logs: List[QueryLog] = []
        lookback_start_time = start_of_day(self._query_log_config.lookback_days)

        for ids in chunks(query_execution_ids, 50):
            raw_response = self._client.batch_get_query_execution(QueryExecutionIds=ids)

            response = BatchGetQueryExecutionResponse(**raw_response)
            for unprocessed in response.UnprocessedQueryExecutionIds or []:
                logger.warning(
                    f"id: {unprocessed.QueryExecutionId}, msg: {unprocessed.ErrorMessage}"
                )

            for query_execution in response.QueryExecutions or []:
                if query_execution.StatementType == "UTILITY":
                    # Skip utility query, e.g. DESC TABLE
                    continue

                query = query_execution.Query
                if not query:
                    continue

                context = query_execution.QueryExecutionContext
                database, schema = (
                    (context.Catalog, context.Database) if context else (None, None)
                )

                start_time = (
                    to_utc_time(query_execution.Status.SubmissionDateTime)
                    if query_execution.Status
                    and query_execution.Status.SubmissionDateTime
                    else None
                )

                if start_time and start_time < lookback_start_time:
                    continue

                tll = extract_table_level_lineage(
                    sql=query,
                    platform=DataPlatform.ATHENA,
                    account=None,
                    default_database=database,
                    default_schema=schema,
                )

                query_log = process_and_init_query_log(
                    query=query,
                    platform=DataPlatform.ATHENA,
                    process_query_config=self._query_log_config.process_query,
                    query_log=PartialQueryLog(
                        duration=(
                            query_execution.Statistics.TotalExecutionTimeInMillis
                            if query_execution.Statistics
                            else None
                        ),
                        sources=tll.sources,
                        targets=tll.targets,
                        start_time=start_time,
                    ),
                    query_id=query_execution.QueryExecutionId,
                )

                if query_log:
                    query_logs.append(query_log)

        return query_logs
