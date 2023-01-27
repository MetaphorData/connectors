from typing import Dict, List

import tableauserverclient as tableau

from metaphor.common.logger import get_logger

logger = get_logger()


def paginate_connection(
    server: tableau.Server, query: str, connection_name: str, batch_size=50
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
        if len(nodes) == 0:
            return result

        result.extend(nodes)
        offset += batch_size
