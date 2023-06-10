from typing import List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import to_person_entity_id
from metaphor.common.logger import get_logger
from metaphor.custom.governance.config import CustomGovernanceConfig
from metaphor.models.metadata_change_event import (
    AssetDescription,
    ColumnTagAssignment,
    Dataset,
    DescriptionAssignment,
    MetadataChangeEvent,
    Ownership,
    OwnershipAssignment,
    TagAssignment,
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

            if len(governance.tags) > 0:
                dataset.tag_assignment = TagAssignment(tag_names=governance.tags)

            if len(governance.column_tags) > 0:
                if dataset.tag_assignment is None:
                    dataset.tag_assignment = TagAssignment()

                dataset.tag_assignment.column_tag_assignments = [
                    ColumnTagAssignment(
                        column_name=column_tag.column, tag_names=column_tag.tags
                    )
                    for column_tag in governance.column_tags
                ]

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
