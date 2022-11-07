from typing import List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import to_person_entity_id
from metaphor.common.logger import get_logger
from metaphor.manual.governance.config import ManualGovernanceConfig
from metaphor.models.metadata_change_event import (
    AssetDescription,
    Dataset,
    DescriptionAssignment,
    MetadataChangeEvent,
    Ownership,
    OwnershipAssignment,
    TagAssignment,
)

logger = get_logger()


class ManualGovernanceExtractor(BaseExtractor):
    """Manual governance extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "ManualGovernanceExtractor":
        return ManualGovernanceExtractor(
            ManualGovernanceConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: ManualGovernanceConfig) -> None:
        super().__init__(config, "Manual governance connector", None)
        self._datasets = config.datasets

    async def extract(self) -> List[MetadataChangeEvent]:
        logger.info("Fetching governance from config")

        datasets = []
        for governance in self._datasets:
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

            if len(governance.descriptions) > 0:
                asset_descriptions = [
                    AssetDescription(
                        description=d.description,
                        author=str(to_person_entity_id(d.email)),
                    )
                    for d in governance.descriptions
                ]

                dataset.description_assignment = DescriptionAssignment(
                    asset_descriptions=asset_descriptions
                )

        return datasets
