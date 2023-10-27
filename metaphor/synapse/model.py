from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class SynapseQueryLog(BaseModel):
    # transaction_id for serverless sql pool
    request_id: str
    session_id: Optional[str] = None
    sql_query: str
    login_name: str
    start_time: datetime
    end_time: datetime
    # in milliseconds
    duration: int
    # in MB
    query_size: Optional[int] = None
    error: Optional[str] = None
    row_count: Optional[int] = None
    query_operation: Optional[str] = None
