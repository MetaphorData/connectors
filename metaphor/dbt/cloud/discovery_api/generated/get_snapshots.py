# Generated by ariadne-codegen
# Source: queries.graphql

from typing import Any, List, Optional

from pydantic import Field

from .base_model import BaseModel
from .enums import TestType


class GetSnapshots(BaseModel):
    environment: "GetSnapshotsEnvironment"


class GetSnapshotsEnvironment(BaseModel):
    applied: Optional["GetSnapshotsEnvironmentApplied"]


class GetSnapshotsEnvironmentApplied(BaseModel):
    snapshots: "GetSnapshotsEnvironmentAppliedSnapshots"


class GetSnapshotsEnvironmentAppliedSnapshots(BaseModel):
    total_count: int = Field(alias="totalCount")
    page_info: "GetSnapshotsEnvironmentAppliedSnapshotsPageInfo" = Field(
        alias="pageInfo"
    )
    edges: List["GetSnapshotsEnvironmentAppliedSnapshotsEdges"]


class GetSnapshotsEnvironmentAppliedSnapshotsPageInfo(BaseModel):
    has_next_page: bool = Field(alias="hasNextPage")
    end_cursor: Optional[str] = Field(alias="endCursor")


class GetSnapshotsEnvironmentAppliedSnapshotsEdges(BaseModel):
    node: "GetSnapshotsEnvironmentAppliedSnapshotsEdgesNode"


class GetSnapshotsEnvironmentAppliedSnapshotsEdgesNode(BaseModel):
    alias: Optional[str]
    catalog: Optional["GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeCatalog"]
    compiled_code: Optional[str] = Field(alias="compiledCode")
    database: Optional[str]
    description: Optional[str]
    environment_id: Any = Field(alias="environmentId")
    meta: Optional[Any]
    name: Optional[str]
    package_name: Optional[str] = Field(alias="packageName")
    raw_code: Optional[str] = Field(alias="rawCode")
    schema_: Optional[str] = Field(alias="schema")
    tags: List[str]
    unique_id: str = Field(alias="uniqueId")
    execution_info: Optional[
        "GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeExecutionInfo"
    ] = Field(alias="executionInfo")
    tests: List["GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeTests"]


class GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeCatalog(BaseModel):
    comment: Optional[str]
    owner: Optional[str]
    columns: Optional[
        List["GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeCatalogColumns"]
    ]


class GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeCatalogColumns(BaseModel):
    comment: Optional[str]
    description: Optional[str]
    index: Optional[int]
    meta: Optional[Any]
    name: Optional[str]
    tags: List[str]
    type: Optional[str]


class GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeExecutionInfo(BaseModel):
    execute_completed_at: Optional[Any] = Field(alias="executeCompletedAt")
    execution_time: Optional[float] = Field(alias="executionTime")
    last_job_definition_id: Optional[Any] = Field(alias="lastJobDefinitionId")
    last_run_id: Optional[Any] = Field(alias="lastRunId")
    last_run_status: Optional[str] = Field(alias="lastRunStatus")


class GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeTests(BaseModel):
    column_name: Optional[str] = Field(alias="columnName")
    description: Optional[str]
    name: Optional[str]
    unique_id: str = Field(alias="uniqueId")
    test_type: TestType = Field(alias="testType")
    execution_info: (
        "GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeTestsExecutionInfo"
    ) = Field(alias="executionInfo")


class GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeTestsExecutionInfo(BaseModel):
    execute_completed_at: Optional[Any] = Field(alias="executeCompletedAt")
    last_run_status: Optional[str] = Field(alias="lastRunStatus")
    last_run_error: Optional[str] = Field(alias="lastRunError")


GetSnapshots.model_rebuild()
GetSnapshotsEnvironment.model_rebuild()
GetSnapshotsEnvironmentApplied.model_rebuild()
GetSnapshotsEnvironmentAppliedSnapshots.model_rebuild()
GetSnapshotsEnvironmentAppliedSnapshotsEdges.model_rebuild()
GetSnapshotsEnvironmentAppliedSnapshotsEdgesNode.model_rebuild()
GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeCatalog.model_rebuild()
GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeTests.model_rebuild()
