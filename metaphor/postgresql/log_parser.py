from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from metaphor.common.logger import get_logger

logger = get_logger()


@dataclass
class ParsedLog:
    log_time: datetime
    user: str
    database: str
    session: str
    log_level: str
    log_body: List[str]


def parse_postgres_log(log: str) -> Optional[ParsedLog]:
    """
    log prefix format: %t:%r:%u@%d:[%p]:
    log message format: [message type]: [log body]
    """
    parts = log.split(":")

    if len(parts) < 8:
        # log did not match the format
        logger.warning(
            f"Not able to parse the log, not enough parts, # of parts: {len(parts)}"
        )
        return None

    user, database = parts[4].split("@")

    return ParsedLog(
        log_time=datetime.strptime(":".join(parts[:3]), "%Y-%m-%d %H:%M:%S %Z").replace(
            tzinfo=timezone.utc
        ),
        log_level=parts[6],
        session=parts[5],
        user=user,
        database=database,
        log_body=parts[7:],
    )
