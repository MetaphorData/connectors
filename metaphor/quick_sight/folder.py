from typing import List

from metaphor.common.hierarchy import create_hierarchy
from metaphor.models.metadata_change_event import AssetPlatform, Hierarchy

DASHBOARD_DIRECTORIES = ["DASHBOARD"]
DATA_SET_DIRECTORIES = ["DATA_SET"]


def create_top_level_folders() -> List[Hierarchy]:
    platform = AssetPlatform.QUICK_SIGHT

    return [
        create_hierarchy(platform, DASHBOARD_DIRECTORIES, "Dashboards"),
        create_hierarchy(platform, DATA_SET_DIRECTORIES, "DataSets"),
    ]
