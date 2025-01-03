import json
from typing import Dict, List, Optional, Tuple, Union

from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.logger import get_logger
from metaphor.common.utils import dedup_lists
from metaphor.dbt.cloud.client import DbtProject
from metaphor.dbt.util import build_system_tags
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    Metric,
    Ownership,
    OwnershipAssignment,
    TagAssignment,
    VirtualView,
)

logger = get_logger()

"""
Maximum number of items to fetch in one request from the discovery API.
"""
DISCOVERY_API_PAGE_LIMIT = 100

"""
Mapping from dbt platform name to DataPlatform if the name differs.
"""
PLATFORM_NAME_MAP = {
    "POSTGRES": "POSTGRESQL",
}


def find_dataset_by_parts(
    datasets: Dict[str, Dataset],
    database: str,
    schema: str,
    name: str,
    platform: DataPlatform,
    account: Optional[str],
) -> Optional[Dataset]:
    """
    Find a dataset in existing datasets dict by its platform, account, database, schema, and name
    """
    for dataset in datasets.values():
        logical_id = dataset.logical_id
        assert logical_id is not None
        if (
            logical_id.platform == platform
            and logical_id.account == account
            and logical_id.name == dataset_normalized_name(database, schema, name)
        ):
            return dataset
    return None


def update_entity_system_tags(
    entity: Union[Metric, VirtualView],
    tags: List[str],
) -> None:
    """
    Updates the entity's system tags. The duplicated tag values are removed.
    """
    if not tags:
        return
    existing_tags = (
        []
        if not entity.system_tags
        else [tag.value for tag in entity.system_tags.tags or [] if tag.value]
    )
    entity.system_tags = build_system_tags(dedup_lists(existing_tags, tags))


def update_entity_ownership_assignments(
    entity: Union[Dataset, VirtualView], ownerships: List[Ownership]
) -> None:
    if not ownerships:
        return
    existing_ownerships = (
        []
        if not entity.ownership_assignment
        else entity.ownership_assignment.ownerships or []
    )
    Ownership.__hash__ = lambda ownership: hash(json.dumps(ownership.to_dict()))  # type: ignore
    entity.ownership_assignment = OwnershipAssignment(
        ownerships=dedup_lists(existing_ownerships, ownerships)
    )


def update_entity_tag_assignments(
    entity: Dataset,
    tag_names: List[str],
) -> None:
    if not tag_names:
        return

    if not entity.tag_assignment:
        entity.tag_assignment = TagAssignment()
    if entity.tag_assignment.tag_names is None:
        entity.tag_assignment.tag_names = []
    entity.tag_assignment.tag_names = dedup_lists(
        entity.tag_assignment.tag_names, tag_names
    )


def extract_platform_and_account(
    project: DbtProject,
) -> Tuple[DataPlatform, Optional[str]]:
    """Get the platform and account from a dbt project"""
    assert project.connection is not None, "Project has no connection"

    type = project.connection.type.upper()
    platform_name = PLATFORM_NAME_MAP.get(type, type)
    assert (
        platform_name in DataPlatform.__members__
    ), f"Invalid platform {platform_name}"
    platform = DataPlatform[platform_name]

    account = (
        project.connection.details.get("account")
        if platform == DataPlatform.SNOWFLAKE
        else None
    )
    return platform, account
