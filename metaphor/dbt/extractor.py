import json
from typing import Dict, List, Optional, Union

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    MetadataChangeEvent,
    VirtualView,
)

from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.dbt.config import DbtRunConfig

from .catalog_parser_v1 import CatalogParserV1
from .generated.dbt_catalog_v1 import DbtCatalog
from .manifest_parser_v3 import ManifestParserV3
from .manifest_parser_v4 import ManifestParserV4

logger = get_logger(__name__)


class DbtExtractor(BaseExtractor):
    """
    dbt metadata extractor
    Using manifest v3 and catalog v1 schema, but backward-compatible with older schema versions
    """

    @staticmethod
    def config_class():
        return DbtRunConfig

    def __init__(self):
        self.platform: DataPlatform = DataPlatform.UNKNOWN
        self.account: Optional[str] = None
        self.docs_base_url: Optional[str] = None
        self.project_source_url: Optional[str] = None
        self._catalog: Optional[DbtCatalog] = None
        self._datasets: Dict[str, Dataset] = {}
        self._virtual_views: Dict[str, VirtualView] = {}

    async def extract(self, config: DbtRunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, DbtExtractor.config_class())

        logger.info("Fetching metadata from DBT repo")
        self.account = config.account
        self.docs_base_url = config.docs_base_url
        self.project_source_url = config.project_source_url

        with open(config.manifest) as file:
            manifest_json = json.load(file)

        manifest_metadata = manifest_json.get("metadata", "")

        platform = manifest_metadata.get("adapter_type", "").upper()
        assert platform in DataPlatform.__members__, f"Invalid data platform {platform}"
        self.platform = DataPlatform[platform]

        schema_version = (
            manifest_metadata.get("dbt_schema_version", "")
            .rsplit("/", 1)[-1]
            .split(".")[0]
        )

        manifest_parser: Union[ManifestParserV3, ManifestParserV4]
        if schema_version in ("v1", "v2", "v3"):
            manifest_parser = ManifestParserV3(
                self.platform,
                self.account,
                self.docs_base_url,
                self.project_source_url,
                self._datasets,
                self._virtual_views,
            )
        elif schema_version == "v4":
            manifest_parser = ManifestParserV4(
                self.platform,
                self.account,
                self.docs_base_url,
                self.project_source_url,
                self._datasets,
                self._virtual_views,
            )
        else:
            raise ValueError(f"unsupported manifest schema '{schema_version}'")

        logger.info(f"parsing manifest.json {schema_version} ...")
        manifest_parser.parse(manifest_json)

        if config.catalog:
            catalog_parser = CatalogParserV1(
                self.platform,
                self.account,
                self.docs_base_url,
                self._datasets,
                self._virtual_views,
            )

            logger.info("parsing catalog.json ...")
            catalog_parser.parse(config.catalog)

        dataset_events = [
            EventUtil.build_dataset_event(d) for d in self._datasets.values()
        ]
        virtual_view_events = [
            EventUtil.build_virtual_view_event(d) for d in self._virtual_views.values()
        ]
        return dataset_events + virtual_view_events
