# Generated by ariadne-codegen
# Source: queries.graphql

from typing import Any, List, Optional

from pydantic import Field

from .base_model import BaseModel
from .enums import RunStatus, TestType


class GetModels(BaseModel):
    environment: "GetModelsEnvironment"


class GetModelsEnvironment(BaseModel):
    applied: Optional["GetModelsEnvironmentApplied"]


class GetModelsEnvironmentApplied(BaseModel):
    models: "GetModelsEnvironmentAppliedModels"


class GetModelsEnvironmentAppliedModels(BaseModel):
    total_count: int = Field(alias="totalCount")
    page_info: "GetModelsEnvironmentAppliedModelsPageInfo" = Field(alias="pageInfo")
    edges: List["GetModelsEnvironmentAppliedModelsEdges"]


class GetModelsEnvironmentAppliedModelsPageInfo(BaseModel):
    has_next_page: bool = Field(alias="hasNextPage")
    end_cursor: Optional[str] = Field(alias="endCursor")


class GetModelsEnvironmentAppliedModelsEdges(BaseModel):
    node: "GetModelsEnvironmentAppliedModelsEdgesNode"


class GetModelsEnvironmentAppliedModelsEdgesNode(BaseModel):
    alias: Optional[str]
    catalog: Optional["GetModelsEnvironmentAppliedModelsEdgesNodeCatalog"]
    compiled_code: Optional[str] = Field(alias="compiledCode")
    database: Optional[str]
    description: Optional[str]
    environment_id: Any = Field(alias="environmentId")
    materialized_type: Optional[str] = Field(alias="materializedType")
    meta: Optional[Any]
    name: Optional[str]
    package_name: Optional[str] = Field(alias="packageName")
    raw_code: Optional[str] = Field(alias="rawCode")
    schema_: Optional[str] = Field(alias="schema")
    tags: List[str]
    unique_id: str = Field(alias="uniqueId")
    execution_info: Optional[
        "GetModelsEnvironmentAppliedModelsEdgesNodeExecutionInfo"
    ] = Field(alias="executionInfo")
    tests: List["GetModelsEnvironmentAppliedModelsEdgesNodeTests"]


class GetModelsEnvironmentAppliedModelsEdgesNodeCatalog(BaseModel):
    comment: Optional[str]
    bytes_stat: Optional[Any] = Field(alias="bytesStat")
    owner: Optional[str]
    row_count_stat: Optional[Any] = Field(alias="rowCountStat")
    columns: Optional[List["GetModelsEnvironmentAppliedModelsEdgesNodeCatalogColumns"]]


class GetModelsEnvironmentAppliedModelsEdgesNodeCatalogColumns(BaseModel):
    comment: Optional[str]
    description: Optional[str]
    name: Optional[str]
    tags: List[str]
    type: Optional[str]
    meta: Optional[Any]


class GetModelsEnvironmentAppliedModelsEdgesNodeExecutionInfo(BaseModel):
    execute_completed_at: Optional[Any] = Field(alias="executeCompletedAt")
    execution_time: Optional[float] = Field(alias="executionTime")
    last_job_definition_id: Optional[Any] = Field(alias="lastJobDefinitionId")
    last_run_id: Optional[Any] = Field(alias="lastRunId")
    last_run_status: Optional[RunStatus] = Field(alias="lastRunStatus")


class GetModelsEnvironmentAppliedModelsEdgesNodeTests(BaseModel):
    column_name: Optional[str] = Field(alias="columnName")
    description: Optional[str]
    name: Optional[str]
    unique_id: str = Field(alias="uniqueId")
    test_type: TestType = Field(alias="testType")
    execution_info: "GetModelsEnvironmentAppliedModelsEdgesNodeTestsExecutionInfo" = (
        Field(alias="executionInfo")
    )


class GetModelsEnvironmentAppliedModelsEdgesNodeTestsExecutionInfo(BaseModel):
    execute_completed_at: Optional[Any] = Field(alias="executeCompletedAt")
    last_run_status: Optional[str] = Field(alias="lastRunStatus")
    last_run_error: Optional[str] = Field(alias="lastRunError")


GetModels.model_rebuild()
GetModelsEnvironment.model_rebuild()
GetModelsEnvironmentApplied.model_rebuild()
GetModelsEnvironmentAppliedModels.model_rebuild()
GetModelsEnvironmentAppliedModelsEdges.model_rebuild()
GetModelsEnvironmentAppliedModelsEdgesNode.model_rebuild()
GetModelsEnvironmentAppliedModelsEdgesNodeCatalog.model_rebuild()
GetModelsEnvironmentAppliedModelsEdgesNodeTests.model_rebuild()