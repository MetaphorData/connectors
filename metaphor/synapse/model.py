from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel


class SynapseWorkspace(BaseModel):
    id: str
    name: str
    type: str
    properties: Any


class SynapseDatabase(BaseModel):
    id: int
    name: str
    create_time: datetime
    collation_name: str


class SynapseColumn(BaseModel):
    name: str
    type: str
    max_length: float
    precision: float
    is_nullable: bool
    is_unique: Optional[bool]
    is_primary_key: Optional[bool]
    is_foreign_key: Optional[bool]


class SynapseTable(BaseModel):
    id: str
    name: str
    schema_name: str
    # detailed type description: https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-objects-transact-sql?view=sql-server-ver16
    type: str
    column_dict: Dict[str, SynapseColumn]
    create_time: datetime
    is_external: bool
    external_source: Optional[str]
    external_file_format: Optional[str]


class SynapseQueryLog(BaseModel):
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
    query_operation: Optional[str]
