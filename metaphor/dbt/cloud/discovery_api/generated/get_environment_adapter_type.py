# Generated by ariadne-codegen
# Source: queries.graphql

from typing import Optional

from pydantic import Field

from .base_model import BaseModel


class GetEnvironmentAdapterType(BaseModel):
    environment: "GetEnvironmentAdapterTypeEnvironment"


class GetEnvironmentAdapterTypeEnvironment(BaseModel):
    adapter_type: Optional[str] = Field(alias="adapterType")
    dbt_project_name: Optional[str] = Field(alias="dbtProjectName")


GetEnvironmentAdapterType.model_rebuild()