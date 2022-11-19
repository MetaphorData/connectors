from datetime import datetime, timezone
from typing import List, Literal, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_extractor import BaseConfig
from metaphor.common.models import DeserializableDatasetLogicalID


@dataclass
class DataQualityMonitorTarget:
    dataset: Optional[str]

    column: Optional[str]


@dataclass
class DataQualityMonitor:
    title: str

    status: Literal["PASSED", "WARNING", "ERROR"]

    targets: List[DataQualityMonitorTarget]

    description: Optional[str] = None

    url: Optional[str] = None

    owner: Optional[str] = None

    severity: Optional[Literal["LOW", "MEDIUM", "HIGH"]] = None

    last_run: Optional[datetime] = datetime.now(tz=timezone.utc)

    value: Optional[float] = None


@dataclass
class DataQuality:
    monitors: List[DataQualityMonitor]

    provider: Optional[Literal["SODA", "LIGHTUP", "BIGEYE"]] = None

    url: Optional[str] = None


@dataclass
class DatasetDataQuality:
    id: DeserializableDatasetLogicalID

    data_quality: DataQuality


@dataclass
class ManualDataQualityConfig(BaseConfig):
    datasets: List[DatasetDataQuality]
