from dataclasses import field as dataclass_field
from typing import Any, Dict, List

from pydantic.dataclasses import dataclass

from metaphor.common.base_extractor import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.models import DeserializableDatasetLogicalID


@dataclass(config=ConnectorConfig)
class DatasetMetadata:
    id: DeserializableDatasetLogicalID

    metadata: Dict[str, Any] = dataclass_field(default_factory=lambda: {})


@dataclass(config=ConnectorConfig)
class CustomMetadataConfig(BaseConfig):
    datasets: List[DatasetMetadata]
