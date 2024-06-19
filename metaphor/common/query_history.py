from typing import Optional, Tuple

from metaphor.common.utils import is_email

# max number of query logs to output in one MCE
DEFAULT_QUERY_LOG_BATCH_SIZE_COUNT = 100


def user_id_or_email(maybe_email: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return a user_id or email depending on if the input contains an email address
    """
    return (None, maybe_email) if is_email(maybe_email) else (maybe_email, None)
