# Generated by ariadne-codegen
# Source: queries.graphql

from typing import List, Optional

from pydantic import Field

from .base_model import BaseModel


class GetJobRunMetrics(BaseModel):
    job: Optional["GetJobRunMetricsJob"]


class GetJobRunMetricsJob(BaseModel):
    metrics: List["GetJobRunMetricsJobMetrics"]


class GetJobRunMetricsJobMetrics(BaseModel):
    package_name: Optional[str] = Field(alias="packageName")
    label: Optional[str]
    description: Optional[str]
    depends_on: List[str] = Field(alias="dependsOn")
    unique_id: str = Field(alias="uniqueId")
    time_grains: Optional[List[str]] = Field(alias="timeGrains")
    timestamp: Optional[str]
    dimensions: List[str]
    filters: List["GetJobRunMetricsJobMetricsFilters"]
    tags: Optional[List[str]]
    type: Optional[str]
    sql: Optional[str]
    expression: Optional[str]
    calculation_method: Optional[str]


class GetJobRunMetricsJobMetricsFilters(BaseModel):
    field: Optional[str]
    operator: Optional[str]
    value: Optional[str]


GetJobRunMetrics.model_rebuild()
GetJobRunMetricsJob.model_rebuild()
GetJobRunMetricsJobMetrics.model_rebuild()
