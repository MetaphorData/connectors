from datetime import datetime, timezone
from typing import List, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_extractor import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.models import DeserializableDatasetLogicalID


@dataclass(config=ConnectorConfig)
class DataQualityMonitorTarget:
    dataset: Optional[str]

    column: Optional[str]


@dataclass(config=ConnectorConfig)
class DataQualityMonitor:
    title: str

    # Change type to Literal["PASSED", "WARNING", "ERROR"] in Python 3.8+
    status: str

    targets: List[DataQualityMonitorTarget]

    description: Optional[str] = None

    url: Optional[str] = None

    owner: Optional[str] = None

    # Change type to Optional[Literal["LOW", "MEDIUM", "HIGH"]] in Python 3.8+
    severity: Optional[str] = None

    last_run: Optional[datetime] = datetime.now(tz=timezone.utc)

    value: Optional[float] = None


@dataclass(config=ConnectorConfig)
class DataQuality:
    monitors: List[DataQualityMonitor]

    # Change type to Optional[Literal["SODA", "LIGHTUP", "BIGEYE"]]
    provider: Optional[str] = None

    url: Optional[str] = None


@dataclass(config=ConnectorConfig)
class DatasetDataQuality:
    id: DeserializableDatasetLogicalID

    data_quality: DataQuality


@dataclass(config=ConnectorConfig)
class CustomDataQualityConfig(BaseConfig):
    datasets: List[DatasetDataQuality]
