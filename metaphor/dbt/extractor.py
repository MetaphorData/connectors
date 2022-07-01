import json
from typing import Collection, Dict, List, Optional, Union

from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    Metric,
    VirtualView,
)

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.dbt.config import DbtRunConfig

from .catalog_parser_v1 import CatalogParserV1
from .generated.dbt_catalog_v1 import DbtCatalog
from .manifest_parser_v3 import ManifestParserV3
from .manifest_parser_v5 import ManifestParserV5

logger = get_logger(__name__)


class DbtExtractor(BaseExtractor):
    """
    dbt metadata extractor
    Using manifest v3 and catalog v1 schema, but backward-compatible with older schema versions
    """

    def platform(self) -> Optional[Platform]:
        return Platform.DBT_MODEL

    def description(self) -> str:
        return "dbt metadata crawler"

    @staticmethod
    def config_class():
        return DbtRunConfig

    def __init__(self):
        self.data_platform: DataPlatform = DataPlatform.UNKNOWN
        self._catalog: Optional[DbtCatalog] = None
        self._datasets: Dict[str, Dataset] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._metrics: Dict[str, Metric] = {}

    async def extract(self, config: DbtRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, DbtExtractor.config_class())

        logger.info("Fetching metadata from DBT repo")

        with open(config.manifest) as file:
            manifest_json = json.load(file)

        manifest_metadata = manifest_json.get("metadata", "")

        platform = manifest_metadata.get("adapter_type", "").upper()
        assert platform in DataPlatform.__members__, f"Invalid data platform {platform}"
        self.data_platform = DataPlatform[platform]

        schema_version = (
            manifest_metadata.get("dbt_schema_version", "")
            .rsplit("/", 1)[-1]
            .split(".")[0]
        )

        manifest_parser: Union[ManifestParserV3, ManifestParserV5]
        if schema_version in ("v1", "v2", "v3"):
            manifest_parser = ManifestParserV3(
                config,
                self.data_platform,
                self._datasets,
                self._virtual_views,
            )
        elif schema_version in ("v4", "v5"):
            manifest_parser = ManifestParserV5(
                config,
                self.data_platform,
                self._datasets,
                self._virtual_views,
                self._metrics,
            )
        else:
            raise ValueError(f"unsupported manifest schema '{schema_version}'")

        logger.info(f"parsing manifest.json {schema_version} ...")
        manifest_parser.parse(manifest_json)

        if config.catalog:
            catalog_parser = CatalogParserV1(
                config,
                self.data_platform,
                self._datasets,
                self._virtual_views,
            )

            logger.info("parsing catalog.json ...")
            catalog_parser.parse(config.catalog)

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._datasets.values())
        entities.extend(self._virtual_views.values())
        entities.extend(self._metrics.values())
        return entities
