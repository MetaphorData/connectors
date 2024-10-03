from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import Field
from pydantic.dataclasses import dataclass


@dataclass
class ColumnMetadata:
    Name: str
    Type: str
    Comment: Optional[str] = None


class TableTypeEnum(Enum):
    EXTERNAL_TABLE = "EXTERNAL_TABLE"
    VIRTUAL_VIEW = "VIRTUAL_VIEW"


@dataclass
class TableMetadata:
    Name: str
    CreateTime: datetime
    Parameters: Dict[str, Optional[str]] = Field(default_factory=dict)
    LastAccessTime: Optional[datetime] = None
    Columns: List[ColumnMetadata] = Field(default_factory=list)
    PartitionKeys: List[ColumnMetadata] = Field(default_factory=list)
    TableType: Optional[str] = None


@dataclass
class QueryExecutionStatus:
    State: Optional[str] = None
    SubmissionDateTime: Optional[datetime] = None


@dataclass
class QueryExecutionStatistics:
    TotalExecutionTimeInMillis: Optional[int] = None


@dataclass
class TypeQueryExecutionContext:
    Database: Optional[str] = None
    Catalog: Optional[str] = None


@dataclass
class QueryExecution:
    QueryExecutionId: str
    Query: Optional[str] = None
    StatementType: Optional[str] = None
    QueryExecutionContext: Optional[TypeQueryExecutionContext] = None
    Status: Optional[QueryExecutionStatus] = None
    Statistics: Optional[QueryExecutionStatistics] = None
    SubstatementType: Optional[str] = None


@dataclass
class UnprocessedQueryExecutionId:
    QueryExecutionId: str
    ErrorCode: Optional[str] = None
    ErrorMessage: Optional[str] = None


@dataclass
class BatchGetQueryExecutionResponse:
    QueryExecutions: Optional[List[QueryExecution]]
    UnprocessedQueryExecutionIds: Optional[List[UnprocessedQueryExecutionId]]
