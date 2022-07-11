from typing import List, Optional

from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    Dataset,
    MetadataChangeEvent,
    Ownership,
    OwnershipAssignment,
    TagAssignment,
)

from metaphor.common.entity_id import to_person_entity_id
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger

from .config import ManualGovernanceConfig

logger = get_logger(__name__)


class ManualGovernanceExtractor(BaseExtractor):
    """Manual governance extractor"""

    def platform(self) -> Optional[Platform]:
        return None

    def description(self) -> str:
        return "Manual governance connector"

    @staticmethod
    def config_class():
        return ManualGovernanceConfig

    async def extract(
        self, config: ManualGovernanceConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, ManualGovernanceExtractor.config_class())
        logger.info("Fetching governance from config")

        datasets = []
        for governance in config.datasets:
            dataset = Dataset(logical_id=governance.id.to_logical_id())
            datasets.append(dataset)

            if len(governance.ownerships) > 0:
                ownerships = [
                    Ownership(
                        contact_designation_name=o.type,
                        person=str(to_person_entity_id(o.email)),
                    )
                    for o in governance.ownerships
                ]

                dataset.ownership_assignment = OwnershipAssignment(
                    ownerships=ownerships
                )

            if len(governance.tags) > 0:
                dataset.tag_assignment = TagAssignment(tag_names=governance.tags)

        return datasets
