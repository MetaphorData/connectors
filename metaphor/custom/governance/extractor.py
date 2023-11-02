from typing import List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import to_person_entity_id
from metaphor.common.logger import get_logger
from metaphor.custom.governance.config import CustomGovernanceConfig
from metaphor.models.metadata_change_event import (
    Dataset,
    MetadataChangeEvent,
    Ownership,
    OwnershipAssignment,
)

logger = get_logger()


class CustomGovernanceExtractor(BaseExtractor):
    """Custom governance extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "CustomGovernanceExtractor":
        return CustomGovernanceExtractor(
            CustomGovernanceConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: CustomGovernanceConfig) -> None:
        super().__init__(config, "Custom governance connector", None)
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
            dataset.description_assignment = governance.to_description_assignment()
            dataset.tag_assignment = governance.to_tag_assignment()

        return datasets
