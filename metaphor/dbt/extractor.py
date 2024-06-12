import json
from typing import Collection, Dict, List, Set

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import add_debug_file, get_logger
from metaphor.dbt.artifact_parser import ArtifactParser
from metaphor.dbt.config import DbtRunConfig
from metaphor.dbt.util import get_data_platform_from_manifest
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
        self._referenced_virtual_views: Set[str] = set()

        add_debug_file(config.manifest)
        if config.run_results:
            add_debug_file(config.run_results)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from DBT repo")

        self._data_platform = get_data_platform_from_manifest(self._manifest)

        with open(self._manifest) as f:
            manifest_json = json.load(f)

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
            self._referenced_virtual_views,
        )
        artifact_parser.parse(manifest_json, run_results_json)

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._datasets.values())
        entities.extend(
            v
            for k, v in self._virtual_views.items()
            if k not in self._referenced_virtual_views
        )
        entities.extend(self._metrics.values())
        return entities
