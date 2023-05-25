import logging
import math
from typing import Collection, Dict, List, Tuple, Union

from pydantic import parse_raw_as

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    to_dataset_entity_id,
    to_dataset_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.common.utils import start_of_day, unique_list
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUpstream,
)
from metaphor.snowflake import auth
from metaphor.snowflake.accessed_object import AccessedObject
from metaphor.snowflake.extractor import DEFAULT_FILTER
from metaphor.snowflake.lineage.config import SnowflakeLineageRunConfig
from metaphor.snowflake.utils import QueryWithParam, async_execute

logger = get_logger()

# disable logging from sql_metadata
logging.getLogger("Parser").setLevel(logging.CRITICAL)


SUPPORTED_OBJECT_DOMAIN_TYPES = (
    "TABLE",
    "VIEW",
    "MATERIALIZED VIEW",
)


class SnowflakeLineageExtractor(BaseExtractor):
    """Snowflake lineage extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "SnowflakeLineageExtractor":
        return SnowflakeLineageExtractor(
            SnowflakeLineageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: SnowflakeLineageRunConfig):
        super().__init__(config, "Snowflake data lineage crawler", Platform.SNOWFLAKE)
        self._account = normalize_snowflake_account(config.account)
        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)
        self._max_concurrency = config.max_concurrency
        self._batch_size = config.batch_size
        self._lookback_days = config.lookback_days
        self._enable_view_lineage = config.enable_view_lineage
        self._enable_lineage_from_history = config.enable_lineage_from_history
        self._include_self_lineage = config.include_self_lineage
        self._config = config

        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching lineage info from Snowflake")

        self._conn = auth.connect(self._config)
        start_date = start_of_day(self._lookback_days)

        with self._conn:
            cursor = self._conn.cursor()

            if self._enable_lineage_from_history:
                logger.info("Fetching access and query history")
                # Join QUERY_HISTORY & ACCESS_HISTORY to include only queries that succeeded.
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM
                        SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q,
                        SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
                    WHERE q.QUERY_ID = a.QUERY_ID
                        AND q.EXECUTION_STATUS = 'SUCCESS'
                        AND ARRAY_SIZE(a.BASE_OBJECTS_ACCESSED) > 0
                        AND ARRAY_SIZE(a.OBJECTS_MODIFIED) > 0
                        AND a.QUERY_START_TIME > %s
                    ORDER BY a.QUERY_START_TIME ASC
                    """,
                    (start_date,),
                )
                res = cursor.fetchone()
                assert res is not None, f"Missing count: {res}"
                count = res[0]
                batches = math.ceil(count / self._batch_size)
                logger.info(f"Total {count} queries, dividing into {batches} batches")

                queries = {
                    str(x): QueryWithParam(
                        f"""
                        SELECT a.BASE_OBJECTS_ACCESSED, a.OBJECTS_MODIFIED, q.QUERY_TEXT
                        FROM
                            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q,
                            SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
                        WHERE q.QUERY_ID = a.QUERY_ID
                            AND q.EXECUTION_STATUS = 'SUCCESS'
                            AND ARRAY_SIZE(a.BASE_OBJECTS_ACCESSED) > 0
                            AND ARRAY_SIZE(a.OBJECTS_MODIFIED) > 0
                            AND a.QUERY_START_TIME > %s
                        ORDER BY a.QUERY_START_TIME ASC
                        LIMIT {self._batch_size} OFFSET %s
                        """,
                        (
                            start_date,
                            x * self._batch_size,
                        ),
                    )
                    for x in range(batches)
                }
                async_execute(
                    self._conn,
                    queries,
                    "fetch_access_logs",
                    self._max_concurrency,
                    self._parse_access_logs,
                )

            if self._enable_view_lineage:
                logger.info("Fetching direct object dependencies")
                cursor.execute(
                    """
                    SELECT REFERENCED_DATABASE, REFERENCED_SCHEMA, REFERENCED_OBJECT_NAME, REFERENCED_OBJECT_DOMAIN,
                        REFERENCING_DATABASE, REFERENCING_SCHEMA, REFERENCING_OBJECT_NAME, REFERENCING_OBJECT_DOMAIN
                    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES;
                    """
                )
                dependencies = cursor.fetchall()
                self._parse_object_dependencies(dependencies)

        return self._datasets.values()

    def _parse_access_logs(self, batch_number: str, access_logs: List[Tuple]) -> None:
        logger.info(f"access logs batch #{batch_number}")
        for base_objects_accessed, objects_modified, query in access_logs:
            try:
                self._parse_access_log(base_objects_accessed, objects_modified, query)
            except Exception:
                logger.exception(
                    "Failed to parse access log.\n"
                    f"BASE_OBJECTS_ACCESSED: {base_objects_accessed}\n"
                    f"OBJECTS_MODIFIED: {objects_modified}"
                )

    def _parse_access_log(
        self, objects_accessed: str, objects_modified: str, query: str
    ) -> None:
        source_datasets = []

        # Extract source tables/views
        source_objects = parse_raw_as(List[AccessedObject], objects_accessed)
        for obj in source_objects:
            if (
                not obj.objectDomain
                or obj.objectDomain.upper() not in SUPPORTED_OBJECT_DOMAIN_TYPES
            ):
                continue

            normalized_name = obj.objectName.lower().replace('"', "")
            entity_id = to_dataset_entity_id(
                normalized_name, DataPlatform.SNOWFLAKE, self._account
            )
            source_datasets.append(str(entity_id))

        source_datasets = unique_list(source_datasets)
        if len(source_datasets) == 0:
            return

        # Assign source tables as upstream of each destination tables
        target_objects = parse_raw_as(List[AccessedObject], objects_modified)
        for obj in target_objects:
            if (
                not obj.objectDomain
                or obj.objectDomain.upper() not in SUPPORTED_OBJECT_DOMAIN_TYPES
            ):
                continue

            parts = obj.objectName.split(".")
            if len(parts) != 3:
                logger.warning(f"Ignore invalid object name: {obj.objectName}")
                continue

            database, schema, name = parts
            normalized_name = dataset_normalized_name(database, schema, name)
            if not self._filter.include_table(database, schema, name):
                logger.debug(f"Excluding table {normalized_name}")
                continue

            logical_id = DatasetLogicalID(
                name=normalized_name,
                account=self._account,
                platform=DataPlatform.SNOWFLAKE,
            )

            filtered_source_datasets = source_datasets.copy()
            if not self._include_self_lineage:
                try:
                    entity_id = to_dataset_entity_id_from_logical_id(logical_id)
                    filtered_source_datasets.remove(str(entity_id))
                except ValueError:
                    # Nothing to remove if there's no self lineage
                    pass

            upstream = DatasetUpstream(
                source_datasets=filtered_source_datasets, transformation=query
            )

            self._datasets[normalized_name] = Dataset(
                logical_id=logical_id, upstream=upstream
            )

    def _parse_object_dependencies(
        self, object_dependencies: Union[List[Tuple], List[Dict]]
    ) -> None:
        for (
            source_db,
            source_schema,
            source_table,
            source_object_domain,
            target_db,
            target_schema,
            target_table,
            target_object_domain,
        ) in object_dependencies:
            if (
                not source_object_domain
                or source_object_domain.upper() not in SUPPORTED_OBJECT_DOMAIN_TYPES
                or not target_object_domain
                or target_object_domain.upper() not in SUPPORTED_OBJECT_DOMAIN_TYPES
            ):
                continue

            source_normalized_name = dataset_normalized_name(
                source_db, source_schema, source_table
            )
            source_entity_id_str = str(
                to_dataset_entity_id(
                    source_normalized_name, DataPlatform.SNOWFLAKE, self._account
                )
            )

            target_normalized_name = dataset_normalized_name(
                target_db, target_schema, target_table
            )

            if not self._filter.include_table(target_db, target_schema, target_table):
                logger.info(f"Excluding table {target_normalized_name}")
                continue

            target_logical_id = DatasetLogicalID(
                name=target_normalized_name,
                account=self._account,
                platform=DataPlatform.SNOWFLAKE,
            )

            if target_normalized_name in self._datasets:
                source_datasets = self._datasets[
                    target_normalized_name
                ].upstream.source_datasets
                if source_entity_id_str not in source_datasets:
                    source_datasets.append(source_entity_id_str)
            else:
                self._datasets[target_normalized_name] = Dataset(
                    logical_id=target_logical_id,
                    upstream=DatasetUpstream(source_datasets=[source_entity_id_str]),
                )
