from datetime import datetime
from typing import Collection, Dict, List, Optional, Tuple

from databricks.sql.client import Connection

from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.utils import start_of_day
from metaphor.unity_catalog.models import (
    CatalogInfo,
    Column,
    ColumnLineage,
    SchemaInfo,
    TableInfo,
    TableLineage,
    Tag,
    VolumeFileInfo,
    VolumeInfo,
)

logger = get_logger()


TableLineageMap = Dict[str, TableLineage]
"""Map a table's full name to its table lineage"""

ColumnLineageMap = Dict[str, ColumnLineage]
"""Map a table's full name to its column lineage"""

IGNORED_HISTORY_OPERATIONS = {
    "ADD CONSTRAINT",
    "CHANGE COLUMN",
    "LIQUID TAGGING",
    "OPTIMIZE",
    "SET TBLPROPERTIES",
}
"""These are the operations that do not modify actual data."""


def list_catalogs(connection: Connection) -> List[CatalogInfo]:
    """
    Fetch catalogs from system.access.information_schema
    See
      - https://docs.databricks.com/en/sql/language-manual/information-schema/catalogs.html
      - https://docs.databricks.com/en/sql/language-manual/information-schema/catalog_tags.html
    """
    catalogs: List[CatalogInfo] = []

    with connection.cursor() as cursor:
        query = """
            WITH c AS (
                SELECT
                    catalog_name,
                    catalog_owner,
                    comment
                FROM system.information_schema.catalogs
                WHERE catalog_name <> 'system'
            ),

            t AS (
                SELECT
                    catalog_name,
                    struct(tag_name, tag_value) as tag
                FROM system.information_schema.catalog_tags
                WHERE catalog_name <> 'system'
            )

            SELECT
                c.catalog_name AS catalog_name,
                first(c.catalog_owner) AS catalog_owner,
                first(c.comment) AS comment,
                collect_list(t.tag) AS tags
            FROM c
            LEFT JOIN t
            ON c.catalog_name = t.catalog_name
            GROUP BY c.catalog_name
            ORDER by c.catalog_name
        """

        try:
            cursor.execute(query)
        except Exception as error:
            logger.exception(f"Failed to list catalogs: {error}")
            return []

        for row in cursor.fetchall():
            catalogs.append(
                CatalogInfo(
                    catalog_name=row["catalog_name"],
                    owner=row["catalog_owner"],
                    comment=row["comment"],
                    tags=Tag.from_row_tags(row["tags"]),
                )
            )

    logger.info(f"Found {len(catalogs)} catalogs")
    json_dump_to_debug_file(catalogs, "list_catalogs.json")
    return catalogs


def list_schemas(connection: Connection, catalog: str) -> List[SchemaInfo]:
    """
    Fetch schemas for a specific catalog from system.access.information_schema
    See
      - https://docs.databricks.com/en/sql/language-manual/information-schema/schemata.html
      - https://docs.databricks.com/en/sql/language-manual/information-schema/schema_tags.html
    """
    schemas: List[SchemaInfo] = []

    with connection.cursor() as cursor:
        query = """
            WITH s AS (
                SELECT
                    catalog_name,
                    schema_name,
                    schema_owner,
                    comment
                FROM system.information_schema.schemata
                WHERE catalog_name = %(catalog)s AND schema_name <> 'information_schema'
            ),

            t AS (
                SELECT
                    catalog_name,
                    schema_name,
                    struct(tag_name, tag_value) as tag
                FROM system.information_schema.schema_tags
                WHERE catalog_name = %(catalog)s AND schema_name <> 'information_schema'
            )

            SELECT
                first(s.catalog_name) AS catalog_name,
                s.schema_name AS schema_name,
                first(s.schema_owner) AS schema_owner,
                first(s.comment) AS comment,
                collect_list(t.tag) AS tags
            FROM s
            LEFT JOIN t
            ON s.catalog_name = t.catalog_name AND s.schema_name = t.schema_name
            GROUP BY s.schema_name
            ORDER by s.schema_name
        """

        try:
            cursor.execute(query, {"catalog": catalog})
        except Exception as error:
            logger.exception(f"Failed to list schemas for {catalog}: {error}")
            return []

        for row in cursor.fetchall():
            schemas.append(
                SchemaInfo(
                    catalog_name=row["catalog_name"],
                    schema_name=row["schema_name"],
                    owner=row["schema_owner"],
                    comment=row["comment"],
                    tags=Tag.from_row_tags(row["tags"]),
                )
            )

    logger.info(f"Found {len(schemas)} schemas from {catalog}")
    json_dump_to_debug_file(schemas, f"list_schemas_{catalog}.json")
    return schemas


def list_tables(connection: Connection, catalog: str, schema: str) -> List[TableInfo]:
    """
    Fetch tables for a specific schema from system.access.information_schema
    See
      - https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html
      - https://docs.databricks.com/en/sql/language-manual/information-schema/views.html
      - https://docs.databricks.com/en/sql/language-manual/information-schema/table_tags.html
      - https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html
      - https://docs.databricks.com/en/sql/language-manual/information-schema/column_tags.html
    """

    tables: List[TableInfo] = []
    with connection.cursor() as cursor:
        query = """
            WITH
            t AS (
                SELECT
                    table_catalog,
                    table_schema,
                    table_name,
                    table_type,
                    table_owner,
                    comment,
                    data_source_format,
                    storage_path,
                    created,
                    created_by,
                    last_altered,
                    last_altered_by
                FROM system.information_schema.tables
                WHERE
                    table_catalog = %(catalog)s AND
                    table_schema = %(schema)s
            ),

            tt AS (
                SELECT
                    catalog_name AS table_catalog,
                    schema_name AS table_schema,
                    table_name AS table_name,
                    collect_list(struct(tag_name, tag_value)) as tags
                FROM system.information_schema.table_tags
                WHERE
                    catalog_name = %(catalog)s AND
                    schema_name = %(schema)s
                GROUP BY catalog_name, schema_name, table_name
            ),

            v AS (
                SELECT
                    table_catalog,
                    table_schema,
                    table_name,
                    view_definition
                FROM system.information_schema.views
                WHERE
                    table_catalog = %(catalog)s AND
                    table_schema = %(schema)s
            ),

            tf AS (
                SELECT
                    t.table_catalog,
                    t.table_schema,
                    t.table_name,
                    t.table_type,
                    t.table_owner,
                    t.comment,
                    t.data_source_format,
                    t.storage_path,
                    t.created,
                    t.created_by,
                    t.last_altered,
                    t.last_altered_by,
                    v.view_definition,
                    tt.tags
                FROM t
                LEFT JOIN v
                ON
                    t.table_catalog = v.table_catalog AND
                    t.table_schema = v.table_schema AND
                    t.table_name = v.table_name
                LEFT JOIN tt
                ON
                    t.table_catalog = tt.table_catalog AND
                    t.table_schema = tt.table_schema AND
                    t.table_name = tt.table_name
            ),

            c AS (
                SELECT
                    table_catalog,
                    table_schema,
                    table_name,
                    column_name,
                    data_type,
                    CASE
                        WHEN numeric_precision IS NOT NULL THEN numeric_precision
                        WHEN datetime_precision IS NOT NULL THEN datetime_precision
                        ELSE NULL
                    END AS data_precision,
                    is_nullable,
                    comment
                FROM system.information_schema.columns
                WHERE
                    table_catalog = %(catalog)s AND
                    table_schema = %(schema)s
            ),

            ct AS (
                SELECT
                    catalog_name AS table_catalog,
                    schema_name AS table_schema,
                    table_name,
                    column_name,
                    collect_list(struct(tag_name, tag_value)) as tags
                FROM system.information_schema.column_tags
                WHERE
                    catalog_name = %(catalog)s AND
                    schema_name = %(schema)s
                GROUP BY catalog_name, schema_name, table_name, column_name
            ),

            cf AS (
                SELECT
                    c.table_catalog,
                    c.table_schema,
                    c.table_name,
                    collect_list(struct(
                        c.column_name,
                        c.data_type,
                        c.data_precision,
                        c.is_nullable,
                        c.comment,
                        ct.tags
                    )) as columns
                FROM c
                LEFT JOIN ct
                ON
                    c.table_catalog = ct.table_catalog AND
                    c.table_schema = ct.table_schema AND
                    c.table_name = ct.table_name AND
                    c.column_name = ct.column_name
                GROUP BY c.table_catalog, c.table_schema, c.table_name
            )

            SELECT
                tf.table_catalog AS catalog_name,
                tf.table_schema AS schema_name,
                tf.table_name AS table_name,
                tf.table_type AS table_type,
                tf.table_owner AS owner,
                tf.comment AS table_comment,
                tf.data_source_format AS data_source_format,
                tf.storage_path AS storage_path,
                tf.created AS created_at,
                tf.created_by AS created_by,
                tf.last_altered as updated_at,
                tf.last_altered_by AS updated_by,
                tf.view_definition AS view_definition,
                tf.tags AS tags,
                cf.columns AS columns
            FROM tf
            LEFT JOIN cf
            ON
                tf.table_catalog = cf.table_catalog AND
                tf.table_schema = cf.table_schema AND
                tf.table_name = cf.table_name
            ORDER by tf.table_catalog, tf.table_schema, tf.table_name
        """

        try:
            cursor.execute(query, {"catalog": catalog, "schema": schema})
        except Exception as error:
            logger.exception(f"Failed to list tables for {catalog}.{schema}: {error}")
            return []

        for row in cursor.fetchall():
            tables.append(TableInfo.from_row(row))

    logger.info(f"Found {len(tables)} tables from {catalog}")
    json_dump_to_debug_file(tables, f"list_tables_{catalog}_{schema}.json")
    return tables


def list_volumes(connection: Connection, catalog: str, schema: str) -> List[VolumeInfo]:
    """
    Fetch volumes for a specific catalog from system.access.information_schema
    See
      - https://docs.databricks.com/en/sql/language-manual/information-schema/volumes.html
      - https://docs.databricks.com/en/sql/language-manual/information-schema/volume_tags.html
    """
    volumes: List[VolumeInfo] = []

    with connection.cursor() as cursor:
        query = """
            WITH v AS (
                SELECT
                    volume_catalog,
                    volume_schema,
                    volume_name,
                    volume_type,
                    volume_owner,
                    comment,
                    created,
                    created_by,
                    last_altered,
                    last_altered_by,
                    storage_location
                FROM system.information_schema.volumes
                WHERE volume_catalog = %(catalog)s AND volume_schema = %(schema)s
            ),

            t AS (
                SELECT
                    catalog_name,
                    schema_name,
                    volume_name,
                    struct(tag_name, tag_value) as tag
                FROM system.information_schema.volume_tags
                WHERE catalog_name = %(catalog)s AND schema_name = %(schema)s
            )

            SELECT
                first(v.volume_catalog) AS volume_catalog,
                first(v.volume_schema) AS volume_schema,
                v.volume_name AS volume_name,
                first(v.volume_type) AS volume_type,
                first(v.volume_owner) AS volume_owner,
                first(v.comment) AS comment,
                first(v.created) AS created,
                first(v.created_by) AS created_by,
                first(v.last_altered) AS last_altered,
                first(v.last_altered_by) AS last_altered_by,
                first(v.storage_location) AS storage_location,
                collect_list(t.tag) AS tags
            FROM v
            LEFT JOIN t
            ON
                v.volume_catalog = t.catalog_name AND
                v.volume_schema = t.schema_name AND
                v.volume_name = t.volume_name
            GROUP BY v.volume_name
            ORDER BY v.volume_name
        """

        try:
            cursor.execute(query, {"catalog": catalog, "schema": schema})
        except Exception as error:
            logger.exception(f"Failed to list volumes for {catalog}.{schema}: {error}")
            return []

        for row in cursor.fetchall():
            volumes.append(
                VolumeInfo(
                    catalog_name=row["volume_catalog"],
                    schema_name=row["volume_schema"],
                    volume_name=row["volume_name"],
                    full_name=f"{row['volume_catalog']}.{row['volume_schema']}.{row['volume_name']}".lower(),
                    volume_type=row["volume_type"],
                    owner=row["volume_owner"],
                    comment=row["comment"],
                    created_at=row["created"],
                    created_by=row["created_by"],
                    updated_at=row["last_altered"],
                    updated_by=row["last_altered_by"],
                    storage_location=row["storage_location"],
                    tags=Tag.from_row_tags(row["tags"]),
                )
            )

    logger.info(f"Found {len(volumes)} volumes from {catalog}.{schema}")
    json_dump_to_debug_file(volumes, f"list_volumes_{catalog}_{schema}.json")
    return volumes


def list_volume_files(
    connection: Connection, volume_info: VolumeInfo
) -> List[VolumeFileInfo]:
    """
    List files in a volume
    See https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-list.html
    """

    catalog_name = volume_info.catalog_name
    schema_name = volume_info.schema_name
    volume_name = volume_info.volume_name

    volume_files: List[VolumeFileInfo] = []

    with connection.cursor() as cursor:
        query = f"LIST '/Volumes/{catalog_name}/{schema_name}/{volume_name}'"

        try:
            cursor.execute(query)
        except Exception as error:
            logger.exception(
                f"Failed to list files in {volume_info.full_name}: {error}"
            )
            return []

        for row in cursor.fetchall():
            volume_files.append(
                VolumeFileInfo(
                    last_updated=row["modification_time"],
                    name=row["name"],
                    path=row["path"],
                    size=float(row["size"]),
                )
            )

    logger.info(f"Found {len(volume_files)} files in {volume_info.full_name}")
    json_dump_to_debug_file(
        volume_files,
        f"list_volume_files_{catalog_name}_{schema_name}_{volume_name}.json",
    )
    return volume_files


def list_table_lineage(
    connection: Connection, catalog: str, schema: str, lookback_days=7
) -> TableLineageMap:
    """
    Fetch table lineage for a specific schema from system.access.table_lineage table
    See https://docs.databricks.com/en/admin/system-tables/lineage.html for more details
    """

    table_lineage: Dict[str, TableLineage] = {}

    with connection.cursor() as cursor:
        query = f"""
            SELECT
                source_table_full_name,
                target_table_full_name
            FROM system.access.table_lineage
            WHERE
                target_table_catalog = '{catalog}' AND
                target_table_schema = '{schema}' AND
                source_table_full_name IS NOT NULL AND
                event_time > date_sub(now(), {lookback_days})
            GROUP BY
                source_table_full_name,
                target_table_full_name
        """

        try:
            cursor.execute(query)
        except Exception as error:
            logger.exception(
                f"Failed to list table lineage for {catalog}.{schema}: {error}"
            )
            return {}

        for source_table, target_table in cursor.fetchall():
            lineage = table_lineage.setdefault(target_table.lower(), TableLineage())
            lineage.upstream_tables.append(source_table.lower())

    logger.info(
        f"Fetched table lineage for {len(table_lineage)} tables in {catalog}.{schema}"
    )
    json_dump_to_debug_file(table_lineage, f"table_lineage_{catalog}_{schema}.json")
    return table_lineage


def list_column_lineage(
    connection: Connection, catalog: str, schema: str, lookback_days=7
) -> ColumnLineageMap:
    """
    Fetch column lineage for a specific schema from system.access.table_lineage table
    See https://docs.databricks.com/en/admin/system-tables/lineage.html for more details
    """
    column_lineage: Dict[str, ColumnLineage] = {}

    with connection.cursor() as cursor:
        query = f"""
            SELECT
                source_table_full_name,
                source_column_name,
                target_table_full_name,
                target_column_name
            FROM system.access.column_lineage
            WHERE
                target_table_catalog = '{catalog}' AND
                target_table_schema = '{schema}' AND
                source_table_full_name IS NOT NULL AND
                event_time > date_sub(now(), {lookback_days})
            GROUP BY
                source_table_full_name,
                source_column_name,
                target_table_full_name,
                target_column_name
        """

        try:
            cursor.execute(query)
        except Exception as error:
            logger.exception(
                f"Failed to list column lineage for {catalog}.{schema}: {error}"
            )
            return {}

        cursor.execute(query)

        for (
            source_table,
            source_column,
            target_table,
            target_column,
        ) in cursor.fetchall():
            lineage = column_lineage.setdefault(target_table.lower(), ColumnLineage())
            columns = lineage.upstream_columns.setdefault(target_column.lower(), [])
            columns.append(
                Column(
                    table_name=source_table.lower(), column_name=source_column.lower()
                )
            )

    logger.info(
        f"Fetched column lineage for {len(column_lineage)} tables in {catalog}.{schema}"
    )
    json_dump_to_debug_file(column_lineage, f"column_lineage_{catalog}_{schema}.json")
    return column_lineage


def list_query_logs(
    connection: Connection, lookback_days: int, excluded_usernames: Collection[str]
):
    """
    Fetch query logs from system.query.history table
    See https://docs.databricks.com/en/admin/system-tables/query-history.html
    """
    start = start_of_day(lookback_days)
    end = start_of_day()

    user_condition = ",".join([f"'{user}'" for user in excluded_usernames])
    user_filter = f"Q.executed_by IN ({user_condition}) AND" if user_condition else ""

    with connection.cursor() as cursor:
        query = f"""
            SELECT
                statement_id as query_id,
                executed_by as email,
                start_time,
                int(total_task_duration_ms/1000) as duration,
                read_rows as rows_read,
                produced_rows as rows_written,
                read_bytes as bytes_read,
                written_bytes as bytes_written,
                statement_type as query_type,
                statement_text as query_text
            FROM system.query.history
            WHERE
                {user_filter}
                execution_status = 'FINISHED' AND
                start_time >= ? AND
                start_time < ?
        """

        try:
            cursor.execute(query, [start, end])
        except Exception as error:
            logger.exception(f"Failed to list query logs: {error}")
            return []

        return cursor.fetchall()


def get_last_refreshed_time(
    connection: Connection,
    table_full_name: str,
    limit: int,
) -> Optional[Tuple[str, datetime]]:
    """
    Retrieve the last refresh time for a table
    See https://docs.databricks.com/en/delta/history.html
    """

    with connection.cursor() as cursor:
        try:
            cursor.execute(f"DESCRIBE HISTORY {table_full_name} LIMIT {limit}")
        except Exception as error:
            logger.error(f"Failed to get history for {table_full_name}: {error}")
            return None

        for history in cursor.fetchall():
            operation = history["operation"]
            if operation not in IGNORED_HISTORY_OPERATIONS:
                logger.info(
                    f"Fetched last refresh time for {table_full_name} ({operation})"
                )
                return (table_full_name, history["timestamp"])

    return None


def get_table_properties(
    connection: Connection,
    table_full_name: str,
) -> Optional[Tuple[str, Dict[str, str]]]:
    """
    Retrieve the properties for a table
    See https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-show-tblproperties.html
    """
    properties: Dict[str, str] = {}
    with connection.cursor() as cursor:
        try:
            cursor.execute(f"SHOW TBLPROPERTIES {table_full_name}")
        except Exception as error:
            logger.error(
                f"Failed to show table properties for {table_full_name}: {error}"
            )
            return None

        for row in cursor.fetchall():
            properties[row["key"]] = row["value"]

    return (table_full_name, properties)
