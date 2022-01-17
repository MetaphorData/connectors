import hashlib
from dataclasses import dataclass
from typing import Optional, Union

from canonicaljson import encode_canonical_json
from metaphor.models.metadata_change_event import (
    DashboardLogicalID,
    DataPlatform,
    DatasetLogicalID,
    EntityType,
    GroupID,
    KnowledgeCardLogicalID,
    PersonLogicalID,
    VirtualViewLogicalID,
    VirtualViewType,
)

from metaphor.common.event_util import EventUtil


@dataclass
class EntityId:
    type: EntityType
    logicalId: Union[
        DatasetLogicalID,
        DashboardLogicalID,
        GroupID,
        KnowledgeCardLogicalID,
        PersonLogicalID,
        VirtualViewLogicalID,
    ]

    def __str__(self) -> str:
        json = encode_canonical_json(EventUtil.clean_nones(self.logicalId.to_dict()))
        return f"{self.type.name}~{hashlib.md5(json).hexdigest().upper()}"

    def __hash__(self):
        return hash(str(self))


def to_dataset_entity_id(
    full_name: str, platform: DataPlatform, account: Optional[str] = None
) -> EntityId:
    """
    converts a dataset name, platform and account into a dataset entity ID
    """
    return EntityId(
        EntityType.DATASET,
        DatasetLogicalID(name=full_name, platform=platform, account=account),
    )


def to_virtual_view_entity_id(name: str, virtualViewType: VirtualViewType) -> EntityId:
    """
    converts a virtual view name and type into a Virtual View entity ID
    """
    return EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name=name, type=virtualViewType),
    )
