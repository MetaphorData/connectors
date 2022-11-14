from datetime import datetime, timezone
from typing import Any, List, Optional

from pydantic import BaseModel


class SynapseDataModel(BaseModel):
    id: str
    name: str
    type: str


class SynapseWorkspace(SynapseDataModel):
    properties: Any


class WorkspaceDatabase(SynapseDataModel):
    properties: Any


class DedicatedSqlPoolSchema(SynapseDataModel):
    pass


class DedicatedSqlPoolColumn(SynapseDataModel):
    properties: Any


class DedicatedSqlPoolTable(SynapseDataModel):
    properties: Any
    sqlSchema: Optional[DedicatedSqlPoolSchema]
    columns: Optional[List]


class SynapseTable(SynapseDataModel):
    properties: Any


class QueryTable(BaseModel):
    # transaction_id for serverless sql pool
    request_id: str
    session_id: Optional[str]
    sql_query: str
    login_name: str
    start_time: datetime
    end_time: datetime
    # in milliseconds
    duration: int
    # in MB
    query_size: Optional[int]
    error: Optional[str]
    row_count: Optional[int]

    @staticmethod
    def to_utc_time(time: datetime) -> "datetime":
        return time.replace(tzinfo=timezone.utc)
