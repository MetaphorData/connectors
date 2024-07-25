from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue
from typing import Collection, Dict, List, Optional, Set, Tuple

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ServicePrincipal
from databricks.sql.client import Connection
from databricks.sql.types import Row

from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.common.utils import is_email, md5_digest, safe_float, start_of_day
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    QueriedDataset,
    QueryLog,
)
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
        cursor.execute(query, [start, end])
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
            logger.exception(f"Failed to get history for {table_full_name}: {error}")
            return None

        for history in cursor.fetchall():
            operation = history["operation"]
            if operation not in IGNORED_HISTORY_OPERATIONS:
                logger.info(
                    f"Fetched last refresh time for {table_full_name} ({operation})"
                )
                return (table_full_name, history["timestamp"])

    return None


def batch_get_last_refreshed_time(
    connection_pool: Queue,
    table_full_names: List[str],
    describe_history_limit: int,
):
    result_map: Dict[str, datetime] = {}

    with ThreadPoolExecutor(max_workers=connection_pool.maxsize) as executor:

        def get_last_refreshed_time_helper(table_full_name: str):
            connection = connection_pool.get()
            result = get_last_refreshed_time(
                connection, table_full_name, describe_history_limit
            )
            connection_pool.put(connection)
            return result

        futures = {
            executor.submit(
                get_last_refreshed_time_helper,
                table_full_name,
            ): table_full_name
            for table_full_name in table_full_names
        }

        for future in as_completed(futures):
            try:
                result = future.result()
                if result is None:
                    continue
                table_full_name, last_refreshed_time = result
                result_map[table_full_name] = last_refreshed_time
            except Exception:
                logger.exception(
                    f"Not able to get refreshed time for {futures[future]}"
                )

    return result_map


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


def create_connection_pool(
    token: str, hostname: str, http_path: str, size: int
) -> Queue:
    """
    Create a pool of connections to allow concurrent querying
    """

    connection_pool: Queue = Queue(maxsize=size)
    for _ in range(connection_pool.maxsize):
        connection_pool.put(create_connection(token, hostname, http_path))

    return connection_pool


def create_api(host: str, token: str) -> WorkspaceClient:
    return WorkspaceClient(host=host, token=token)


def list_service_principals(api: WorkspaceClient) -> Dict[str, ServicePrincipal]:
    """
    List all service principals
    See https://docs.databricks.com/api/workspace/groups/list
    """

    # See https://databricks-sdk-py.readthedocs.io/en/latest/dbdataclasses/iam.html#databricks.sdk.service.iam.Group
    excluded_attributes = "entitlements,groups,roles,schemas"

    try:
        principals = list(
            api.service_principals.list(excluded_attributes=excluded_attributes)
        )
        json_dump_to_debug_file(principals, "list-principals.json")

        principal_map: Dict[str, ServicePrincipal] = {}
        for p in principals:
            if p.application_id is not None:
                principal_map[p.application_id] = p

        return principal_map
    except Exception as e:
        logger.error(f"Failed to list principals: {e}")
        return {}


def find_qualified_dataset(dataset: QueriedDataset, datasets: Dict[str, Dataset]):
    if dataset.database and dataset.schema:
        # No need
        return dataset

    # See if there's only one fully qualified dataset
    candidates = set(
        key
        for key in datasets.keys()
        if key.endswith(
            dataset_normalized_name(
                dataset.database if dataset.database else None,
                dataset.schema if dataset.schema else None,
                dataset.table if dataset.table else None,
            )
        )
    )

    if len(candidates) == 1:
        # If there's only one dataset that matches the partially qualified
        # dataset, we know for sure this is the one we're looking for.
        key = list(candidates)[0]
        found = datasets[key]

        if found.structure:
            name = dataset_normalized_name(
                found.structure.database,
                found.structure.schema,
                found.structure.table,
            )
            id = str(
                to_dataset_entity_id(
                    name,
                    platform=DataPlatform.UNITY_CATALOG,
                    account=None,
                )
            )
            return QueriedDataset(
                database=found.structure.database,
                schema=found.structure.schema,
                table=found.structure.table,
                id=id,
            )
    # Cannot find matching fully qualified dataset, this probably
    # isn't an actual table
    return None


def to_query_log_with_tll(
    row: Row,
    service_principals: Dict[str, ServicePrincipal],
    datasets: Dict[str, Dataset],
):
    query_id = row["query_id"]

    sql = row["query_text"]
    ttl = extract_table_level_lineage(
        sql,
        platform=DataPlatform.UNITY_CATALOG,
        account=None,
        statement_type=row["query_type"],
        query_id=query_id,
    )

    sources: List[QueriedDataset] = []
    targets: List[QueriedDataset] = []

    email = None
    user_id = None
    if is_email(row["email"]):
        email = row["email"]
    elif row["email"] in service_principals:
        user_id = service_principals[row["email"]].display_name
    else:
        user_id = row["email"]

    for source in ttl.sources:
        found = find_qualified_dataset(source, datasets)
        if found:
            sources.append(found)

    for target in ttl.targets:
        found = find_qualified_dataset(target, datasets)
        if found:
            targets.append(found)

    return QueryLog(
        id=f"{DataPlatform.UNITY_CATALOG.name}:{query_id}",
        query_id=query_id,
        platform=DataPlatform.UNITY_CATALOG,
        email=email,
        user_id=user_id,
        start_time=row["start_time"],
        duration=safe_float(row["duration"]),
        rows_read=safe_float(row["rows_read"]),
        rows_written=safe_float(row["rows_written"]),
        bytes_read=safe_float(row["bytes_read"]),
        bytes_written=safe_float(row["bytes_written"]),
        sources=sources,
        targets=targets,
        sql=sql,
        sql_hash=md5_digest(sql.encode("utf-8")),
    )


def get_query_logs(
    connection: Connection,
    lookback_days: int,
    excluded_usernames: Set[str],
    service_principals: Dict[str, ServicePrincipal],
    datasets: Dict[str, Dataset],
):
    rows = list_query_logs(
        connection,
        lookback_days,
        excluded_usernames,
    )

    count = 0
    logger.info(f"{len(rows)} queries to fetch")
    for row in rows:
        res = to_query_log_with_tll(row, service_principals, datasets)
        count += 1
        if count % 1000 == 0:
            logger.info(f"Fetched {count} queries")
        yield res
