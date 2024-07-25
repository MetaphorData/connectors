from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.unity_catalog.config import UnityCatalogRunConfig


@dataclass(config=ConnectorConfig)
class UnityCatalogProfileRunConfig(UnityCatalogRunConfig):
    analyze_if_no_statistics: bool = False
