from dataclasses import dataclass
from typing import Dict, List, Optional

from metaphor.common.logger import get_logger


@dataclass
class FolderMetadata:
    id: str
    name: str
    parent_id: Optional[str] = None


FolderMap = Dict[str, FolderMetadata]

logger = get_logger()


def build_directories(folder_id: str, folder_map: FolderMap) -> List[str]:
    directories: List[str] = []

    while True:
        folder = folder_map.get(folder_id)
        if folder is None:
            logger.error(f"Invalid folder ID: {folder_id}")
            return []

        directories.insert(0, folder.id)

        if folder.parent_id is None:
            return directories

        folder_id = folder.parent_id
