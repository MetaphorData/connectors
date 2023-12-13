import json
from typing import Collection, Dict, List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import add_debug_file, get_logger
from metaphor.dbt.artifact_parser import ArtifactParser
from metaphor.dbt.config import DbtRunConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    Metric,
    VirtualView,
)

logger = get_logger()


class DbtExtractor(BaseExtractor):
    """
    dbt metadata extractor
    Using manifest v3 and catalog v1 schema, but backward-compatible with older schema versions
    """

    _description = "dbt metadata crawler"
    _platform = Platform.DBT_MODEL

    @staticmethod
    def from_config_file(config_file: str) -> "DbtExtractor":
        return DbtExtractor(DbtRunConfig.from_yaml_file(config_file))

    def __init__(self, config: DbtRunConfig):
        super().__init__(config)
        self._manifest = config.manifest
        self._run_results = config.run_results
        self._config = config

        self._data_platform = DataPlatform.UNKNOWN
        self._datasets: Dict[str, Dataset] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._metrics: Dict[str, Metric] = {}

        add_debug_file(config.manifest)
        if config.run_results:
            add_debug_file(config.run_results)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from DBT repo")

        with open(self._manifest) as file:
            manifest_json = json.load(file)

        manifest_metadata = manifest_json.get("metadata", {})
        platform = manifest_metadata.get("adapter_type", "").upper()
        assert platform in DataPlatform.__members__, f"Invalid data platform {platform}"
        self._data_platform = DataPlatform[platform]

        run_results_json = None
        if self._run_results is not None:
            with open(self._run_results) as file:
                run_results_json = json.load(file)

        artifact_parser = ArtifactParser(
            self._config,
            self._data_platform,
            self._datasets,
            self._virtual_views,
            self._metrics,
        )
        artifact_parser.parse(manifest_json, run_results_json)

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._datasets.values())
        entities.extend(self._virtual_views.values())
        entities.extend(self._metrics.values())
        return entities
