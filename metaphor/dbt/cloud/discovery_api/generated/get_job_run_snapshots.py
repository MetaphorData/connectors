# Generated by ariadne-codegen
# Source: queries.graphql

from typing import Any, List, Optional

from pydantic import Field

from .base_model import BaseModel


class GetJobRunSnapshots(BaseModel):
    job: Optional["GetJobRunSnapshotsJob"]


class GetJobRunSnapshotsJob(BaseModel):
    snapshots: List["GetJobRunSnapshotsJobSnapshots"]


class GetJobRunSnapshotsJobSnapshots(BaseModel):
    alias: Optional[str]
    columns: Optional[List["GetJobRunSnapshotsJobSnapshotsColumns"]]
    comment: Optional[str]
    compile_completed_at: Optional[Any] = Field(alias="compileCompletedAt")
    compiled_code: Optional[str] = Field(alias="compiledCode")
    compiled_sql: Optional[str] = Field(alias="compiledSql")
    database: Optional[str]
    description: Optional[str]
    environment_id: Any = Field(alias="environmentId")
    meta: Optional[Any]
    name: Optional[str]
    owner: Optional[str]
    package_name: Optional[str] = Field(alias="packageName")
    raw_code: Optional[str] = Field(alias="rawCode")
    raw_sql: Optional[str] = Field(alias="rawSql")
    schema_: Optional[str] = Field(alias="schema")
    tags: Optional[List[str]]
    unique_id: str = Field(alias="uniqueId")


class GetJobRunSnapshotsJobSnapshotsColumns(BaseModel):
    comment: Optional[str]
    description: Optional[str]
    index: Optional[int]
    meta: Optional[Any]
    name: Optional[str]
    tags: List[str]
    type: Optional[str]


GetJobRunSnapshots.model_rebuild()
GetJobRunSnapshotsJob.model_rebuild()
GetJobRunSnapshotsJobSnapshots.model_rebuild()
