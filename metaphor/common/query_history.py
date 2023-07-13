from typing import List

from metaphor.common.utils import chunks
from metaphor.models.metadata_change_event import QueryLog, QueryLogs

# max number of query logs to output in one MCE
DEFAULT_QUERY_LOG_OUTPUT_SIZE = 100


def chunk_query_logs(query_logs: List[QueryLog]) -> List[QueryLogs]:
    """
    divide query logs into batches to put in MCE
    """
    return [
        QueryLogs(logs=chunk)
        for chunk in chunks(query_logs, DEFAULT_QUERY_LOG_OUTPUT_SIZE)
    ]
