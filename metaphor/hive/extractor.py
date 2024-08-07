from typing import Any, Collection, Dict, Iterable, List, Optional, Tuple

from pyhive import hive

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import build_schema_field
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.hive.config import HiveRunConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    EntityType,
    FieldStatistics,
    MaterializationType,
    SchemaField,
    SchemaType,
    SQLSchema,
)

logger = get_logger()

NUMERIC_TYPES = {
    "TINYINT",
    "SMALLINT",
    "INT",
    "INTEGER",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "DOUBLE PRECISION",
    "DECIMAL",
    "NUMERIC",
}


class HiveExtractor(BaseExtractor):
    """Hive metadata extractor"""

    _description = "Hive metadata crawler"
    _platform = Platform.HIVE

    @staticmethod
    def from_config_file(config_file: str) -> "HiveExtractor":
        return HiveExtractor(HiveRunConfig.from_yaml_file(config_file))

    def __init__(self, config: HiveRunConfig) -> None:
        super().__init__(config)
        self._config = config

    @staticmethod
    def get_connection(**kwargs) -> hive.Connection:
        return hive.connect(
            **kwargs,
            configuration={
                "hive.txn.manager": "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager",
                "hive.support.concurrency": "true",
                "hive.enforce.bucketing": "true",
                "hive.exec.dynamic.partition.mode": "nonstrict",
            },
        )

    @staticmethod
    def extract_names_from_cursor(cursor: Iterable[Tuple]) -> List[str]:
        return [tup[0] for tup in cursor]

    def _extract_table(
        self, database: str, table: str, materialization: MaterializationType
    ) -> Dataset:
        with self._connection.cursor() as cursor:
            normalized_name = dataset_normalized_name(schema=database, table=table)
            dataset = Dataset(
                entity_type=EntityType.DATASET,
                logical_id=DatasetLogicalID(
                    name=normalized_name,
                    platform=DataPlatform.HIVE,
                ),
                structure=DatasetStructure(
                    schema=database,
                    table=table,
                ),
            )
            fields: List[SchemaField] = []
            cursor.execute(f"describe {database}.{table}")
            for field_name, field_type, comment in cursor:
                fields.append(build_schema_field(field_name, field_type, comment))

            cursor.execute(f"show create table {database}.{table}")
            table_schema = "\n".join(
                line for line in HiveExtractor.extract_names_from_cursor(cursor)
            )

            dataset_schema = None
            if fields or table_schema:
                dataset_schema = DatasetSchema()
                if fields:
                    dataset_schema.fields = fields
                if table_schema:
                    dataset_schema.schema_type = SchemaType.SQL
                    dataset_schema.sql_schema = SQLSchema(
                        materialization=materialization,
                        table_schema=table_schema,
                    )
            dataset.schema = dataset_schema

            if materialization in {
                MaterializationType.TABLE,
                MaterializationType.MATERIALIZED_VIEW,
            }:
                dataset.statistics = self._extract_table_stats(database, table, fields)
            return dataset

    @staticmethod
    def _is_numeric_field(field: SchemaField) -> bool:
        return (
            field.native_type is not None and field.native_type.upper() in NUMERIC_TYPES
        )

    def _extract_field_stats(  # noqa C091
        self,
        database: str,
        table: str,
        field: SchemaField,
    ) -> FieldStatistics:
        with self._connection.cursor() as cursor:
            raw_field_statistics: Dict[str, Any] = {
                "fieldPath": field.field_name,
            }

            chosen_stats: Dict[str, str] = {}
            if self._config.column_statistics.null_count:
                chosen_stats["num_nulls"] = "nullValueCount"
            if self._config.column_statistics.min_value:
                chosen_stats["min"] = "minValue"
            if self._config.column_statistics.max_value:
                chosen_stats["max"] = "maxValue"
            if self._config.column_statistics.unique_count:
                chosen_stats["distinct_count"] = "distinctValueCount"

            if chosen_stats:
                # Gotta extract column stats calculated by Hive
                cursor.execute(
                    f"describe formatted {database}.{table} {field.field_name}"
                )
                for row in cursor:
                    field_stats_key = chosen_stats.get(row[0])
                    if field_stats_key:
                        try:
                            raw_field_statistics[field_stats_key] = float(row[1])
                        except Exception:
                            if HiveExtractor._is_numeric_field(field):
                                logger.warning(
                                    f"Cannot find {field_stats_key} for field {field.field_name}"
                                )

            def _calculate_by_hand(function: str) -> Optional[float]:
                try:
                    cursor.execute(
                        f"select {function}({field.field_name}) from {database}.{table}"
                    )
                    return float(next(cursor)[0])
                except Exception:
                    logger.exception(
                        f"Cannot calculate {function} for field {field.field_name}"
                    )
                    return None

            if field.native_type and field.native_type.upper() in NUMERIC_TYPES:
                if self._config.column_statistics.avg_value:
                    raw_field_statistics["average"] = _calculate_by_hand("avg")
                if self._config.column_statistics.std_dev:
                    raw_field_statistics["stdDev"] = _calculate_by_hand("std")

            return FieldStatistics.from_dict(raw_field_statistics)

    def _extract_table_stats(
        self, database: str, table: str, fields: List[SchemaField]
    ):
        with self._connection.cursor() as cursor:
            statement = f"analyze table {database}.{table} compute statistics"
            if self._config.column_statistics.should_calculate:
                statement += " for columns"
            cursor.execute(statement)
            cursor.execute(f"describe formatted {database}.{table}")
            raw_table_stats = list(cursor)
            table_size = next(
                (
                    float(str(r[-1]).strip())
                    for r in raw_table_stats
                    if r[1] and "totalSize" in r[1]
                ),
                None,
            )
            num_rows = next(
                (
                    float(str(r[-1]).strip())
                    for r in raw_table_stats
                    if r[1] and "numRows" in r[1]
                ),
                None,
            )

            dataset_statistics = to_dataset_statistics(
                rows=num_rows, size_bytes=table_size
            )

            field_statistics = None
            if self._config.column_statistics.should_calculate:
                field_statistics = [
                    self._extract_field_stats(database, table, field)
                    for field in fields
                ]

            dataset_statistics.field_statistics = field_statistics
            return dataset_statistics

    def _extract_database(self, database: str) -> List[Dataset]:
        with self._connection.cursor() as cursor:
            datasets = []

            # Hive considers views as tables, but we have to treat them differently!
            cursor.execute(f"show tables in {database}")
            all_tables = set(HiveExtractor.extract_names_from_cursor(cursor))

            cursor.execute(f"show views in {database}")
            views = set(HiveExtractor.extract_names_from_cursor(cursor))

            cursor.execute(f"show materialized views in {database}")
            materialized_views = set(HiveExtractor.extract_names_from_cursor(cursor))

            tables = {
                x for x in all_tables if x not in views and x not in materialized_views
            }

            for table in tables:
                datasets.append(
                    self._extract_table(database, table, MaterializationType.TABLE)
                )

            for view in views:
                datasets.append(
                    self._extract_table(database, view, MaterializationType.VIEW)
                )

            for materialized_view in materialized_views:
                datasets.append(
                    self._extract_table(
                        database,
                        materialized_view,
                        MaterializationType.MATERIALIZED_VIEW,
                    )
                )

            return datasets

    async def extract(self) -> Collection[ENTITY_TYPES]:
        self._connection = HiveExtractor.get_connection(**self._config.connect_kwargs)
        entities: List[ENTITY_TYPES] = []
        with self._connection.cursor() as cursor:
            cursor.execute("show databases")
            for database in HiveExtractor.extract_names_from_cursor(cursor):
                entities.extend(self._extract_database(database))
        return entities
