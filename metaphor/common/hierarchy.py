from typing import List

from metaphor.models.metadata_change_event import (
    AssetPlatform,
    Hierarchy,
    HierarchyInfo,
    HierarchyLogicalID,
    HierarchyType,
)


def create_hierarchy(
    platform: AssetPlatform,
    path: List[str],
    name: str = "",
    hierarchy_type: HierarchyType = HierarchyType.VIRTUAL_HIERARCHY,
) -> Hierarchy:
    """
    Create a hierarchy with name
    """
    return Hierarchy(
        logical_id=HierarchyLogicalID(
            path=[platform.value] + path,
        ),
        hierarchy_info=(
            HierarchyInfo(
                name=name,
                type=hierarchy_type,
            )
            if name
            else None
        ),
    )
