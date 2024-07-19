from datetime import datetime
from typing import Collection, Dict, Optional

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sql.client import Connection

from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.utils import start_of_day
from metaphor.unity_catalog.models import Column, ColumnLineage, TableLineage

logger = get_logger()

# Map a table's full name to its table lineage
TableLineageMap = Dict[str, TableLineage]

# Map a table's full name to its column lineage
ColumnLineageMap = Dict[str, ColumnLineage]

IGNORED_HISTORY_OPERATIONS = {
    "ADD CONSTRAINT",
    "CHANGE COLUMN",
    "LIQUID TAGGING",
    "OPTIMIZE",
    "SET TBLPROPERTIES",
}
"""These are the operations that do not modify actual data."""


def list_table_lineage(
    connection: Connection, catalog: str, schema: str, lookback_days=7
) -> TableLineageMap:
    """
    Fetch table lineage for a specific schema from system.access.table_lineage table
    See https://docs.databricks.com/en/admin/system-tables/lineage.html for more details
    """

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
        cursor.execute(query)

        table_lineage: Dict[str, TableLineage] = {}
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
        cursor.execute(query)

        column_lineage: Dict[str, ColumnLineage] = {}
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


def list_query_log(
    connection: Connection, lookback_days: int, excluded_usernames: Collection[str]
):
    """
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
        cursor.execute(query, [start, end])
        return cursor.fetchall()


def get_last_refreshed_time(
    connection: Connection, table_full_name: str, limit: int
) -> Optional[datetime]:
    """
    Retrieve the last refresh time for a table
    See https://docs.databricks.com/en/delta/history.html
    """

    with connection.cursor() as cursor:
        try:
            cursor.execute("DESCRIBE HISTORY ? LIMIT ?", [table_full_name, limit])
        except Exception as error:
            logger.exception(f"Failed to get history for {table_full_name}: {error}")
            return None

        for history in cursor.fetchall():
            if history["operation"] not in IGNORED_HISTORY_OPERATIONS:
                return history["timestamp"]

    return None


SPECIAL_CHARACTERS = "&*{}[],=-()+;'\"`"
"""
The special characters mentioned in Databricks documentation are:
- `
- `-`

The following characters are not allowed:
- `.`
- ` `
- `/`

See https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html for reference.
"""


def escape_special_characters(name: str) -> str:
    """
    When referencing UC names in SQL, you must use backticks to escape
    names that contain special characters such as hyphens (-).
    """
    if any(c in name for c in SPECIAL_CHARACTERS):
        return f"`{name}`"
    return name


def create_connection(
    token: str,
    server_hostname: Optional[str] = None,
    http_path: Optional[str] = None,
) -> Connection:
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=token,
    )


def create_api(host: str, token: str) -> WorkspaceClient:
    return WorkspaceClient(host=host, token=token)
