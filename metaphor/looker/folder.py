from dataclasses import dataclass
from typing import Dict, List, Optional

from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import (
    DashboardPlatform,
    Hierarchy,
    HierarchyInfo,
    HierarchyLogicalID,
    HierarchyType,
)


@dataclass
class FolderMetadata:
    id: str
    name: str
    parent_id: Optional[str] = None


FolderMap = Dict[str, FolderMetadata]

logger = get_logger()


def build_directories(
    folder_id: str,
    folder_map: FolderMap,
    folder_hierarchies: Dict[str, Hierarchy],
) -> List[str]:
    directories: List[str] = []

    while True:
        folder = folder_map.get(folder_id)
        if folder is None:
            logger.error(f"Invalid folder ID: {folder_id}")
            return []

        directories.insert(0, folder.id)

        if folder.parent_id is None:
            break

        folder_id = folder.parent_id

    _build_hierarchies(directories, folder_map, folder_hierarchies)

    return directories


def _build_hierarchies(
    directories: List[str],
    folder_map: FolderMap,
    folder_hierarchies: Dict[str, Hierarchy],
):
    for i, folder_id in enumerate(directories):
        folder = folder_map.get(folder_id)
        if folder_id in folder_hierarchies or folder is None:
            continue

        hierarchy = Hierarchy(
            logical_id=HierarchyLogicalID(
                path=[DashboardPlatform.LOOKER.value] + directories[: i + 1]
            ),
            hierarchy_info=HierarchyInfo(
                type=HierarchyType.LOOKER_FOLDER, name=folder.name
            ),
        )

        folder_hierarchies[folder_id] = hierarchy
