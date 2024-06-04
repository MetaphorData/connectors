from typing import Dict, List

import tableauserverclient as tableau

from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.tableau.query import (
    CustomSqlTable,
    WorkbookQueryResponse,
    custom_sql_graphql_query,
    workbooks_graphql_query,
)

logger = get_logger()


def _paginate_connection(
    server: tableau.Server, query: str, connection_name: str, batch_size
) -> List[Dict]:
    """Return all the nodes from GraphQL connection through pagination"""

    offset = 0
    result: List[Dict] = []

    while True:
        logger.info(f"Querying {connection_name} with offset {offset}")
        resp = server.metadata.query(query, {"first": batch_size, "offset": offset})
        if resp.get("errors"):
            logger.error(f"Error when querying {connection_name}: {resp.get('errors')}")

        nodes = resp["data"][connection_name]["nodes"]
        result.extend(nodes)

        if len(nodes) < batch_size:
            return result

        offset += batch_size


def fetch_workbooks(server: tableau.Server, batch_size):
    # fetch workbook related info from Metadata GraphQL API
    workbooks = _paginate_connection(
        server, workbooks_graphql_query, "workbooksConnection", batch_size
    )
    json_dump_to_debug_file(workbooks, "graphql_workbooks.json")
    logger.info(f"Found {len(workbooks)} workbooks.")
    return [WorkbookQueryResponse.model_validate(workbook) for workbook in workbooks]


def fetch_custom_sql_tables(server: tableau.Server, batch_size):
    # fetch custom SQL tables from Metadata GraphQL API
    custom_sql_tables = _paginate_connection(
        server, custom_sql_graphql_query, "customSQLTablesConnection", batch_size
    )

    json_dump_to_debug_file(custom_sql_tables, "graphql_custom_sql_tables.json")
    logger.info(f"Found {len(custom_sql_tables)} custom SQL tables.")
    return [CustomSqlTable.model_validate(table) for table in custom_sql_tables]
