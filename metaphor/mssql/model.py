from datetime import datetime
from typing import Dict, Optional

from metaphor.common.models import V1CompatBaseModel


class MssqlConnectConfig(V1CompatBaseModel):
    endpoint: str
    username: str
    password: str


class MssqlDatabase(V1CompatBaseModel):
    id: int
    name: str
    create_time: datetime
    collation_name: str


class MssqlColumn(V1CompatBaseModel):
    name: str
    type: str
    max_length: float
    precision: float
    is_nullable: bool
    is_unique: Optional[bool] = None
    is_primary_key: Optional[bool] = None


class MssqlForeignKey(V1CompatBaseModel):
    name: str
    table_id: str
    column_name: str
    referenced_table_id: str
    referenced_column: str


class MssqlTable(V1CompatBaseModel):
    id: str
    name: str
    schema_name: str
    type: str
    column_dict: Dict[str, MssqlColumn]
    create_time: datetime
    is_external: bool
    external_source: Optional[str] = None
    external_file_format: Optional[str] = None


class MssqlQueryLog(V1CompatBaseModel):
    request_id: str
    session_id: Optional[str] = None
    sql_query: str
    login_name: str
    start_time: datetime
    end_time: datetime
    duration_in_ms: int
    query_size_in_mb: Optional[int] = None
    error: Optional[str] = None
    row_count: Optional[int] = None
    query_operation: Optional[str] = None
