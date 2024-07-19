import datetime
from typing import Dict, Optional

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import QueryFilter, TimeRange
from databricks.sql.client import Connection
from databricks.sql.exc import Error

from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.unity_catalog.config import UnityCatalogQueryLogConfig
from metaphor.unity_catalog.models import Column, ColumnLineage, TableLineage

logger = get_logger()

# Map a table's full name to its table lineage
TableLineageMap = Dict[str, TableLineage]

# Map a table's full name to its column lineage
ColumnLineageMap = Dict[str, ColumnLineage]


def system_access_schema_enabled(connection: Connection):
    """
    Check if system.access schema is enabled
    See https://docs.databricks.com/en/admin/system-tables/index.html#enable-system-table-schemas
    """
    with connection.cursor() as cursor:
        query = "SELECT source_table_full_name FROM system.access.table_lineage LIMIT 1"
        try:
            cursor.execute(query)
            cursor.fetchone()
        except Error:
            return False

    return True


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


def build_query_log_filter_by(
    config: UnityCatalogQueryLogConfig,
    client: WorkspaceClient,
) -> QueryFilter:
    end_time = datetime.datetime.now(tz=datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(days=config.lookback_days)

    query_filter = QueryFilter(
        query_start_time_range=TimeRange(
            end_time_ms=int(end_time.timestamp() * 1000),
            start_time_ms=int(start_time.timestamp() * 1000),
        )
    )
    if config.excluded_usernames:
        query_filter.user_ids = [
            int(user.id)
            for user in client.users.list()
            if user.id and user.user_name not in config.excluded_usernames
        ]

    return query_filter


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
