# Generated by ariadne-codegen
# Source: queries.graphql

from typing import Any, List, Optional

from pydantic import Field

from .base_model import BaseModel


class GetJobRunMacros(BaseModel):
    job: Optional["GetJobRunMacrosJob"]


class GetJobRunMacrosJob(BaseModel):
    macros: List["GetJobRunMacrosJobMacros"]


class GetJobRunMacrosJobMacros(BaseModel):
    depends_on: List[str] = Field(alias="dependsOn")
    description: Optional[str]
    environment_id: Any = Field(alias="environmentId")
    macro_sql: Optional[str] = Field(alias="macroSql")
    meta: Optional[Any]
    name: Optional[str]
    package_name: Optional[str] = Field(alias="packageName")
    unique_id: str = Field(alias="uniqueId")


GetJobRunMacros.model_rebuild()
GetJobRunMacrosJob.model_rebuild()