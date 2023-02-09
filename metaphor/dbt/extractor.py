import json
from typing import Collection, Dict, List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import add_debug_file, get_logger
from metaphor.dbt.config import DbtRunConfig
from metaphor.dbt.manifest_parser import ManifestParser
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    Metric,
    VirtualView,
)

from .catalog_parser_v1 import CatalogParserV1

logger = get_logger()


class DbtExtractor(BaseExtractor):
    """
    dbt metadata extractor
    Using manifest v3 and catalog v1 schema, but backward-compatible with older schema versions
    """

    @staticmethod
    def from_config_file(config_file: str) -> "DbtExtractor":
        return DbtExtractor(DbtRunConfig.from_yaml_file(config_file))

    def __init__(self, config: DbtRunConfig):
        super().__init__(config, "dbt metadata crawler", Platform.DBT_MODEL)
        self._manifest = config.manifest
        self._catalog = config.catalog
        self._config = config

        self._data_platform = DataPlatform.UNKNOWN
        self._datasets: Dict[str, Dataset] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._metrics: Dict[str, Metric] = {}

        add_debug_file(config.manifest)
        if config.catalog:
            add_debug_file(config.catalog)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from DBT repo")

        with open(self._manifest) as file:
            manifest_json = json.load(file)

        manifest_metadata = manifest_json.get("metadata", {})
        platform = manifest_metadata.get("adapter_type", "").upper()
        assert platform in DataPlatform.__members__, f"Invalid data platform {platform}"
        self._data_platform = DataPlatform[platform]

        manifest_parser = ManifestParser(
            self._config,
            self._data_platform,
            self._datasets,
            self._virtual_views,
            self._metrics,
        )
        manifest_parser.parse(manifest_json)

        if self._catalog:
            catalog_parser = CatalogParserV1(
                self._config,
                self._data_platform,
                self._datasets,
                self._virtual_views,
            )

            logger.info("parsing catalog.json ...")
            catalog_parser.parse(self._catalog)

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._datasets.values())
        entities.extend(self._virtual_views.values())
        entities.extend(self._metrics.values())
        return entities
