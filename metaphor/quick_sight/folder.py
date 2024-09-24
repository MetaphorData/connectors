from typing import List

from metaphor.models.metadata_change_event import (
    DashboardPlatform,
    Hierarchy,
    HierarchyInfo,
    HierarchyLogicalID,
    HierarchyType,
)

DASHBOARD_DIRECTORIES = ["DASHBOARD"]
DATA_SET_DIRECTORIES = ["DATA_SET"]


def _create_virtual_hierarchy(name: str, path: List[str]) -> Hierarchy:
    return Hierarchy(
        logical_id=HierarchyLogicalID(path=[DashboardPlatform.QUICK_SIGHT.name] + path),
        hierarchy_info=HierarchyInfo(name=name, type=HierarchyType.VIRTUAL_HIERARCHY),
    )


def create_top_level_folders() -> List[Hierarchy]:
    return [
        _create_virtual_hierarchy("Dashboards", DASHBOARD_DIRECTORIES),
        _create_virtual_hierarchy("DataSets", DATA_SET_DIRECTORIES),
    ]
