# mypy: ignore-errors

# generated by datamodel-codegen:
#   filename:  https://schemas.getdbt.com/dbt/run-results/v4.json
#   timestamp: 2023-11-06T08:28:10+00:00

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict
from typing_extensions import Literal


class BaseArtifactMetadata(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    dbt_schema_version: str
    dbt_version: Optional[str] = '1.0.0b2'
    generated_at: Optional[datetime] = '2021-11-02T20:18:06.799863Z'
    invocation_id: Optional[str] = None
    env: Optional[Dict[str, str]] = {}


class TimingInfo(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    name: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class FreshnessMetadata(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    dbt_schema_version: Optional[str] = 'https://schemas.getdbt.com/dbt/sources/v3.json'
    dbt_version: Optional[str] = '1.0.0b2'
    generated_at: Optional[datetime] = '2021-11-02T20:18:06.796684Z'
    invocation_id: Optional[str] = None
    env: Optional[Dict[str, str]] = {}


class SourceFreshnessRuntimeError(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    unique_id: str
    error: Optional[Union[str, int]] = None
    status: Literal['runtime error']


class Time(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    count: Optional[int] = None
    period: Optional[Literal['minute', 'hour', 'day']] = None


class RunResultOutput(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    status: Union[
        Literal['success', 'error', 'skipped'],
        Literal['pass', 'error', 'fail', 'warn', 'skipped'],
        Literal['pass', 'warn', 'error', 'runtime error'],
    ]
    timing: List[TimingInfo]
    thread_id: str
    execution_time: float
    adapter_response: Dict[str, Any]
    message: Optional[str] = None
    failures: Optional[int] = None
    unique_id: str


class FreshnessThreshold(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    warn_after: Optional[Time] = {'count': None, 'period': None}
    error_after: Optional[Time] = {'count': None, 'period': None}
    filter: Optional[str] = None


class DbtRunResults(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    metadata: BaseArtifactMetadata
    results: List[RunResultOutput]
    elapsed_time: float
    args: Optional[Dict[str, Any]] = {}


class SourceFreshnessOutput(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    unique_id: str
    max_loaded_at: datetime
    snapshotted_at: datetime
    max_loaded_at_time_ago_in_s: float
    status: Literal['pass', 'warn', 'error', 'runtime error']
    criteria: FreshnessThreshold
    adapter_response: Dict[str, Any]
    timing: List[TimingInfo]
    thread_id: str
    execution_time: float