import datetime
from collections import defaultdict
from dataclasses import dataclass
from typing import Collection, Dict, Iterator, List

from trino.auth import BasicAuthentication, JWTAuthentication
from trino.dbapi import connect

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import build_schema_field
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.utils import md5_digest
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    EntityType,
    EntityUpstream,
    MaterializationType,
    QueryLog,
    SchemaField,
    SchemaType,
    SQLSchema,
)
from metaphor.trino.config import TrinoRunConfig

logger = get_logger()

# Filter out "system" database & all "information_schema" schemas
DEFAULT_FILTER: DatasetFilter = DatasetFilter(
    excludes={
        "system": None,
        "*": {"information_schema": None},
    }
)


@dataclass(frozen=True)
class _Table:
    catalog: str
    schema: str
    name: str

    def __repr__(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.name}"

    @property
    def normalized_name(self) -> str:
        return dataset_normalized_name(self.catalog, self.schema, self.name)


class TrinoExtractor(BaseExtractor):
    """Trino metadata crawler"""

    _description = "Trino metadata crawler"
    _platform = Platform.TRINO

    @staticmethod
    def from_config_file(config_file: str) -> "TrinoExtractor":
        return TrinoExtractor(TrinoRunConfig.from_yaml_file(config_file))

    def __init__(self, config: TrinoRunConfig) -> None:
        super().__init__(config)
        self._config = config
        if config.password:
            auth = BasicAuthentication(config.username, config.password)
        elif config.token:
            auth = JWTAuthentication(config.token)
        else:
            auth = None
        self._conn = connect(
            host=config.host,
            port=config.port,
            user=config.username,
            auth=auth,
            http_scheme="https" if config.enable_tls else "http",
        )
        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Trino")

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._extract_datasets())
        entities.extend(self._extract_materialized_views())
        return entities

    def collect_query_logs(self) -> Iterator[QueryLog]:
        cursor = self._conn.cursor()
        cursor.execute(
            'SELECT query_id, user, query, started, "end" FROM system.runtime.queries'
        )
        query_log_count = 0
        for query_id, user, query, started, end in cursor.fetchall():
            log = QueryLog(
                id=f"{DataPlatform.TRINO.name}:{query_id}",
                query_id=query_id,
                sql=query,
                sql_hash=md5_digest(query.encode("utf-8")),
                start_time=started,
                duration=(
                    (end - started).total_seconds()
                    if isinstance(end, datetime.datetime)
                    else None
                ),
                user_id=user,
                platform=DataPlatform.TRINO,
            )
            yield log
            query_log_count += 1
        logger.info(f"Wrote {query_log_count} QueryLog")

    def _extract_materialized_views(self) -> List[Dataset]:
        materialized_views: List[Dataset] = []
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT catalog_name, schema_name, name, storage_catalog, storage_schema, storage_table, definition FROM system.metadata.materialized_views"
        )
        for (
            catalog,
            schema,
            name,
            table_catalog,
            table_schema,
            table_name,
            sql,
        ) in cursor.fetchall():
            normalized_name = dataset_normalized_name(catalog, schema, name)
            materialized_view = Dataset(
                logical_id=DatasetLogicalID(
                    name=normalized_name, platform=DataPlatform.TRINO
                ),
                entity_type=EntityType.DATASET,
                dataset_id=str(
                    to_dataset_entity_id(normalized_name, DataPlatform.TRINO)
                ),
                entity_upstream=EntityUpstream(
                    source_entities=[
                        str(
                            to_dataset_entity_id(
                                dataset_normalized_name(
                                    table_catalog, table_schema, table_name
                                ),
                                DataPlatform.TRINO,
                            )
                        )
                    ]
                ),
                schema=DatasetSchema(
                    raw_schema=sql,
                    schema_type=SchemaType.SQL,
                    sql_schema=SQLSchema(
                        materialization=MaterializationType.MATERIALIZED_VIEW,
                        table_schema=sql,
                    ),
                ),
            )
            materialized_views.append(materialized_view)
        return materialized_views

    def _extract_datasets(self) -> List[Dataset]:
        datasets: List[Dataset] = []
        table_fields: Dict[_Table, List[SchemaField]] = defaultdict(list)

        cursor = self._conn.cursor()
        cursor.execute("SELECT catalog_name FROM system.metadata.catalogs")
        catalogs: List[str] = [row[0] for row in cursor.fetchall()]
        for catalog in catalogs:
            cursor.execute(
                f"SELECT table_schema, table_name, column_name, is_nullable, data_type FROM {catalog}.information_schema.columns"
            )
            rows = cursor.fetchall()
            for schema, name, column_name, column_nullable, column_type in rows:
                table_fields[_Table(catalog, schema, name)].append(
                    build_schema_field(
                        column_name, column_type, None, column_nullable == "YES"
                    )
                )
            for table, fields in table_fields.items():
                if not self._filter.include_table(
                    table.catalog, table.schema, table.name
                ):
                    logger.info(f"Ignore table: {table} due to filter config")
                    continue
                datasets.append(
                    Dataset(
                        logical_id=DatasetLogicalID(
                            name=table.normalized_name, platform=DataPlatform.TRINO
                        ),
                        entity_type=EntityType.DATASET,
                        dataset_id=str(
                            to_dataset_entity_id(
                                table.normalized_name, DataPlatform.TRINO
                            )
                        ),
                        schema=DatasetSchema(fields=fields),
                    )
                )
        return datasets
