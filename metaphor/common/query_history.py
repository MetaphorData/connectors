from typing import List, Optional, Tuple

from metaphor.common.utils import chunks, is_email
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


def user_id_or_email(maybe_email: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return a user_id or email depending on if the input contains an email address
    """
    return (None, maybe_email) if is_email(maybe_email) else (maybe_email, None)
