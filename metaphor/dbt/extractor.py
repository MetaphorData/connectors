import json
from typing import Collection, Dict, List, Union

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.dbt.config import DbtRunConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    Metric,
    VirtualView,
)

from .catalog_parser_v1 import CatalogParserV1
from .manifest_parser_v3 import ManifestParserV3
from .manifest_parser_v5 import ManifestParserV5
from .manifest_parser_v6 import ManifestParserV6
from .manifest_parser_v7 import ManifestParserV7

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

    async def extract(self) -> Collection[ENTITY_TYPES]:

        logger.info("Fetching metadata from DBT repo")

        with open(self._manifest) as file:
            manifest_json = json.load(file)

        manifest_metadata = manifest_json.get("metadata", "")

        platform = manifest_metadata.get("adapter_type", "").upper()
        assert platform in DataPlatform.__members__, f"Invalid data platform {platform}"
        self._data_platform = DataPlatform[platform]

        schema_version = (
            manifest_metadata.get("dbt_schema_version", "")
            .rsplit("/", 1)[-1]
            .split(".")[0]
        )

        manifest_parser: Union[
            ManifestParserV3, ManifestParserV5, ManifestParserV6, ManifestParserV7
        ]
        if schema_version in ("v1", "v2", "v3"):
            manifest_parser = ManifestParserV3(
                self._config,
                self._data_platform,
                self._datasets,
                self._virtual_views,
            )
        elif schema_version in ("v4", "v5"):
            manifest_parser = ManifestParserV5(
                self._config,
                self._data_platform,
                self._datasets,
                self._virtual_views,
                self._metrics,
            )
        elif schema_version == "v6":
            manifest_parser = ManifestParserV6(
                self._config,
                self._data_platform,
                self._datasets,
                self._virtual_views,
                self._metrics,
            )
        elif schema_version == "v7":
            manifest_parser = ManifestParserV7(
                self._config,
                self._data_platform,
                self._datasets,
                self._virtual_views,
                self._metrics,
            )
        else:
            raise ValueError(f"unsupported manifest schema '{schema_version}'")

        logger.info(f"parsing manifest.json {schema_version} ...")
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
