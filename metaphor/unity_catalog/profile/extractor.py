import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from typing import Collection, List, Optional, Tuple

try:
    from databricks.sdk.service.catalog import ColumnTypeName, TableInfo, TableType
except ImportError:
    print("Please install metaphor[unity_catalog] extra\n")
    raise

from databricks.sql.client import Connection

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import normalize_full_dataset_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import build_field_statistics
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.utils import safe_float
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    DatasetStatistics,
)
from metaphor.unity_catalog.profile.config import UnityCatalogProfileRunConfig
from metaphor.unity_catalog.utils import (
    create_api,
    create_connection_pool,
    escape_special_characters,
)

logger = get_logger()

# Filter out "system" database & all "information_schema" schemas
DEFAULT_FILTER: DatasetFilter = DatasetFilter(
    excludes={
        "system": None,
        "*": {"information_schema": None},
    }
)

NON_MODIFICATION_OPERATIONS = {
    "SET TBLPROPERTIES",
    "ADD CONSTRAINT",
}
"""These are the operations that do not count as modifications."""

NUMERIC_TYPES = {
    ColumnTypeName.DECIMAL,
    ColumnTypeName.DOUBLE,
    ColumnTypeName.FLOAT,
    ColumnTypeName.INT,
    ColumnTypeName.LONG,
    ColumnTypeName.SHORT,
}

TABLE_TYPES_TO_PROFILE = {
    TableType.EXTERNAL,
    TableType.EXTERNAL_SHALLOW_CLONE,
    TableType.MANAGED,
    TableType.MANAGED_SHALLOW_CLONE,
}


class UnityCatalogProfileExtractor(BaseExtractor):
    """Unity Catalog data profile extractor"""

    _description = "Unity Catalog data profile crawler"
    _platform = Platform.UNITY_CATALOG

    @staticmethod
    def from_config_file(config_file: str) -> "UnityCatalogProfileExtractor":
        return UnityCatalogProfileExtractor(
            UnityCatalogProfileRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: UnityCatalogProfileRunConfig):
        super().__init__(config)
        self._api = create_api(f"https://{config.hostname}", config.token)

        self._token = config.token
        self._hostname = config.hostname
        self._http_path = config.http_path
        self._max_concurrency = config.max_concurrency

        # Profiling config
        self._analyze_if_no_statistics = config.analyze_if_no_statistics

        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(
            f"Fetching dataset statistics from Unity Catalog, analyze_if_no_statistics: {self._analyze_if_no_statistics}"
        )

        tables = self._get_tables()

        connection_pool = create_connection_pool(
            token=self._token,
            hostname=self._hostname,
            http_path=self._http_path,
            size=self._max_concurrency,
        )
        return self._profile(connection_pool, tables)

    def _profile(
        self, connection_pool: Queue, tables: List[TableInfo]
    ) -> List[Dataset]:
        def profile(table_info: TableInfo):
            dataset = self._init_dataset(table_info)

            connection = connection_pool.get()

            dataset_statistics, field_statistics = (
                self._get_statistics_from_analyzed_table(connection, table_info)
            )

            connection_pool.put(connection)

            dataset.statistics = dataset_statistics
            dataset.field_statistics = field_statistics

            return dataset

        logger.info(f"Profiling {len(tables)} tables")

        entities = []
        with ThreadPoolExecutor(max_workers=connection_pool.maxsize) as executor:
            futures = {executor.submit(profile, table): table for table in tables}

            for future in as_completed(futures):
                table = futures[future]
                try:
                    entities.append(future.result())
                except Exception:
                    logger.exception(f"Not able to profile {table.full_name}")

        return entities

    def _get_tables(self) -> List[TableInfo]:
        catalogs = [
            catalog_info.name
            for catalog_info in self._api.catalogs.list()
            if catalog_info.name and self._filter.include_database(catalog_info.name)
        ]

        tables: List[TableInfo] = []
        for catalog in catalogs:
            schemas = [
                schema_info.name
                for schema_info in self._api.schemas.list(catalog)
                if schema_info.name
                and self._filter.include_schema(catalog, schema_info.name)
            ]
            for schema in schemas:
                table_infos = [
                    table_info
                    for table_info in self._api.tables.list(catalog, schema)
                    if table_info.name
                    and self._filter.include_table(catalog, schema, table_info.name)
                ]
                for table_info in table_infos:
                    table_type = table_info.table_type
                    if table_type not in TABLE_TYPES_TO_PROFILE:
                        continue
                    tables.append(table_info)
        return tables

    def _init_dataset(self, table_info: TableInfo) -> Dataset:
        assert table_info.full_name
        return Dataset(
            logical_id=DatasetLogicalID(
                name=normalize_full_dataset_name(table_info.full_name),
                platform=DataPlatform.UNITY_CATALOG,
            ),
        )

    @staticmethod
    def _get_statistics(
        table_name: str, cursor
    ) -> Tuple[Optional[float], Optional[float]]:
        cursor.execute(f"DESCRIBE TABLE EXTENDED {table_name}")

        rows = cursor.fetchall()
        statistics = next((row for row in rows if row.col_name == "Statistics"), None)
        if statistics:
            # `statistics.data_type` looks like "X bytes, Y rows"
            matches = (float(x) for x in re.findall(r"(\d+)", statistics.data_type))
            return (next(matches, None), next(matches, None))
        return None, None

    def _get_statistics_from_analyzed_table(
        self,
        connection: Connection,
        table_info: TableInfo,
    ) -> Tuple[Optional[DatasetStatistics], Optional[DatasetFieldStatistics]]:
        assert table_info.full_name

        start_time = time.time()

        escaped_name = escape_special_characters(table_info.full_name)

        dataset_statistics = DatasetStatistics()

        # Gather this first, if this is nonempty in the end then we actually save
        # this to `Dataset`.
        field_statistics = DatasetFieldStatistics(field_statistics=[])

        assert field_statistics.field_statistics is not None

        # Gather numeric columns
        numeric_columns = [
            column.name
            for column in (table_info.columns or [])
            if column.type_name in NUMERIC_TYPES and column.name
        ]

        analyze_query = f"ANALYZE TABLE {escaped_name} COMPUTE STATISTICS"
        if numeric_columns:
            analyze_query += f" FOR COLUMNS {', '.join(numeric_columns)}"

        record_count = None
        with connection.cursor() as cursor:
            try:
                # Check if we need to run analyze
                if self._analyze_if_no_statistics:
                    _, record_count = UnityCatalogProfileExtractor._get_statistics(
                        escaped_name, cursor
                    )
                    if record_count is None:
                        # This can take a while
                        cursor.execute(analyze_query)

                data_size_bytes, record_count = (
                    UnityCatalogProfileExtractor._get_statistics(escaped_name, cursor)
                )

            except Exception:
                logger.exception(f"not able to get statistics for {escaped_name}")
                return None, None

            dataset_statistics.data_size_bytes = data_size_bytes
            dataset_statistics.record_count = record_count

            for numeric_column in numeric_columns:
                # Parse numeric column stats
                try:
                    cursor.execute(f"DESCRIBE EXTENDED {escaped_name} {numeric_column}")
                    column_details = cursor.fetchall()
                except Exception:
                    logger.exception(
                        f"not able to get column stat for {escaped_name}.{numeric_column}"
                    )
                    continue

                def get_value_from_row(key: str) -> Optional[float]:
                    value = next(
                        (
                            row.info_value
                            for row in column_details
                            if row.info_name == key
                        ),
                        None,
                    )
                    if value:
                        if value in ["NULL", "Infinity"]:
                            return None
                        return safe_float(value)
                    return value

                field_statistics.field_statistics.append(
                    build_field_statistics(
                        numeric_column,
                        get_value_from_row("distinct_count"),
                        get_value_from_row("num_nulls"),
                        None,
                        get_value_from_row("min"),
                        get_value_from_row("max"),
                    )
                )

        logger.info(
            f"Profiled {table_info.full_name} "
            f"({dataset_statistics.data_size_bytes or 0:.0f} bytes, "
            f"{dataset_statistics.record_count or 0:.0f} rows) "
            f"in {(time.time() - start_time):.1f} seconds"
        )

        return (
            dataset_statistics,
            None if not field_statistics.field_statistics else field_statistics,
        )
