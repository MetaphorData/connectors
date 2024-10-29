from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue
from typing import Dict, List, Optional, Set

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ServicePrincipal
from databricks.sql.client import Connection
from databricks.sql.types import Row

from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.sql.process_query.config import ProcessQueryConfig
from metaphor.common.sql.query_log import PartialQueryLog, process_and_init_query_log
from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.common.utils import is_email, safe_float
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    QueriedDataset,
    SystemTag,
    SystemTags,
    SystemTagSource,
)
from metaphor.unity_catalog.models import Tag
from metaphor.unity_catalog.queries import (
    get_last_refreshed_time,
    get_table_properties,
    list_query_logs,
)

logger = get_logger()


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


def batch_get_table_properties(
    connection_pool: Queue,
    table_full_names: List[str],
) -> Dict[str, Dict[str, str]]:
    result_map: Dict[str, Dict[str, str]] = {}

    with ThreadPoolExecutor(max_workers=connection_pool.maxsize) as executor:

        def get_table_properties_helper(table_full_name: str):
            connection = connection_pool.get()
            result = get_table_properties(connection, table_full_name)
            connection_pool.put(connection)
            return result

        futures = {
            executor.submit(
                get_table_properties_helper,
                table_full_name,
            ): table_full_name
            for table_full_name in table_full_names
        }

        for future in as_completed(futures):
            try:
                result = future.result()
                if result is None:
                    continue

                table_full_name, properties = result
                result_map[table_full_name] = properties
            except Exception:
                logger.exception(
                    f"Not able to get table properties for {futures[future]}"
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


def _to_query_log_with_tll(
    row: Row,
    service_principals: Dict[str, ServicePrincipal],
    datasets: Dict[str, Dataset],
    process_query_config: ProcessQueryConfig,
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

    return process_and_init_query_log(
        query=sql,
        platform=DataPlatform.UNITY_CATALOG,
        process_query_config=process_query_config,
        query_log=PartialQueryLog(
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
        ),
        query_id=query_id,
    )


def get_query_logs(
    connection: Connection,
    lookback_days: int,
    excluded_usernames: Set[str],
    service_principals: Dict[str, ServicePrincipal],
    datasets: Dict[str, Dataset],
    process_query_config: ProcessQueryConfig,
):
    rows = list_query_logs(
        connection,
        lookback_days,
        excluded_usernames,
    )

    count = 0
    logger.info(f"{len(rows)} queries to fetch")
    for row in rows:
        res = _to_query_log_with_tll(
            row, service_principals, datasets, process_query_config
        )
        if res is not None:
            count += 1
            if count % 1000 == 0:
                logger.info(f"Fetched {count} queries")
            yield res


def to_system_tags(tags: List[Tag]) -> SystemTags:
    return SystemTags(
        tags=[
            SystemTag(
                key=tag.key if tag.value else None,
                value=tag.value if tag.value else tag.key,
                system_tag_source=SystemTagSource.UNITY_CATALOG,
            )
            for tag in tags
        ],
    )
