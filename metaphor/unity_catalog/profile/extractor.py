import logging
import re
from typing import Collection, List, Optional, Tuple

from metaphor.unity_catalog.profile.config import UnityCatalogProfileRunConfig
from metaphor.unity_catalog.profile.utils import escape_special_characters

try:
    from databricks import sql
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import ColumnTypeName, TableInfo, TableType
    from databricks.sdk.service.sql import EndpointInfo
    from databricks.sql.client import Connection
except ImportError:
    print("Please install metaphor[unity_catalog] extra\n")
    raise


from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import normalize_full_dataset_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    DatasetStatistics,
    FieldStatistics,
)
from metaphor.unity_catalog.extractor import DEFAULT_FILTER, UnityCatalogExtractor

logger = get_logger()

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

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
        self._token = config.token
        self._host = config.host
        self._api = UnityCatalogExtractor.create_api(config.host, config.token)
        self._connection = UnityCatalogProfileExtractor.create_connection(
            self._api, config.token, config.warehouse_id
        )
        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching data profile from Unity Catalog")
        entities: List[ENTITY_TYPES] = []
        catalogs = [
            catalog_info.name
            for catalog_info in self._api.catalogs.list()
            if self._filter.include_database(catalog_info.name)
        ]
        for catalog in catalogs:
            schemas = [
                schema_info.name
                for schema_info in self._api.schemas.list(catalog)
                if self._filter.include_schema(catalog, schema_info.name)
            ]
            for schema in schemas:
                table_infos = [
                    table_info
                    for table_info in self._api.tables.list(catalog, schema)
                    if self._filter.include_table(catalog, schema, table_info.name)
                ]
                for table_info in table_infos:
                    dataset = self._init_dataset(table_info)
                    if table_info.table_type is not TableType.VIEW:
                        dataset_statistics, field_statistics = self._get_statistics(
                            table_info
                        )
                        dataset.statistics = dataset_statistics
                        dataset.field_statistics = field_statistics
                    entities.append(dataset)
                    logger.info(f"Fetched: {table_info.full_name}")
        return entities

    def _init_dataset(self, table_info: TableInfo) -> Dataset:
        return Dataset(
            logical_id=DatasetLogicalID(
                name=normalize_full_dataset_name(table_info.full_name),
                platform=DataPlatform.UNITY_CATALOG,
            ),
        )

    def _get_statistics(
        self, table_info: TableInfo
    ) -> Tuple[DatasetStatistics, Optional[DatasetFieldStatistics]]:
        dataset_statistics = DatasetStatistics()

        # Gather this first, if this is nonempty in the end then we actually save
        # this to `Dataset`.
        field_statistics = DatasetFieldStatistics(field_statistics=[])
        assert field_statistics.field_statistics is not None

        with self._connection.cursor() as cursor:
            escaped_name = escape_special_characters(table_info.full_name)
            # Gather numeric columns
            numeric_columns = [
                column.name
                for column in (table_info.columns or [])
                if column.type_name in NUMERIC_TYPES and column.name
            ]

            analyze_query = f"ANALYZE TABLE {escaped_name} COMPUTE STATISTICS"
            if numeric_columns:
                analyze_query += f" FOR COLUMNS {', '.join(numeric_columns)}"

            # This can take a while
            cursor.execute(analyze_query)
            cursor.execute(f"DESCRIBE TABLE EXTENDED {escaped_name}")
            rows = cursor.fetchall()
            statistics = next(
                (row for row in rows if row.col_name == "Statistics"), None
            )
            if statistics:
                # `statistics.data_type` looks like "X bytes, Y rows"
                bytes_count, row_count = [
                    float(x) for x in re.findall(r"(\d+)", statistics.data_type)
                ]
                dataset_statistics.data_size_bytes = bytes_count
                dataset_statistics.record_count = row_count

            for numeric_column in numeric_columns:
                # Parse numeric column stats
                cursor.execute(f"DESCRIBE EXTENDED {escaped_name} {numeric_column}")
                column_details = cursor.fetchall()

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
                        if value == "NULL":
                            return None
                        return float(value)
                    return value

                stats = FieldStatistics(
                    distinct_value_count=get_value_from_row("distinct_count"),
                    field_path=numeric_column,
                    max_value=get_value_from_row("max"),
                    min_value=get_value_from_row("min"),
                    null_value_count=get_value_from_row("num_nulls"),
                )
                field_statistics.field_statistics.append(stats)

            cursor.execute(f"DESCRIBE HISTORY {escaped_name}")
            for history in cursor.fetchall():
                if history["operation"] not in NON_MODIFICATION_OPERATIONS:
                    dataset_statistics.last_updated = history["timestamp"]
                    break

        return (
            dataset_statistics,
            None if not field_statistics.field_statistics else field_statistics,
        )

    @staticmethod
    def create_connection(
        client: WorkspaceClient, token: str, warehouse_id: Optional[str]
    ) -> Connection:
        endpoints = list(client.warehouses.list())
        if not endpoints:
            raise ValueError("No valid warehouse found")
        endpoint_info: EndpointInfo = endpoints[0]
        if warehouse_id:
            try:
                endpoint_info = client.warehouses.get(warehouse_id)
            except Exception:
                raise ValueError(f"Invalid warehouse id: {warehouse_id}")
        return sql.connect(
            server_hostname=endpoint_info.odbc_params.hostname,
            http_path=endpoint_info.odbc_params.path,
            access_token=token,
        )
