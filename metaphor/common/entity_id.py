import hashlib
from dataclasses import dataclass
from typing import Union

from canonicaljson import encode_canonical_json
from metaphor.models.metadata_change_event import (
    DashboardLogicalID,
    DatasetLogicalID,
    EntityType,
    GroupID,
    PersonLogicalID,
)


@dataclass
class EntityId:
    type: EntityType
    logicalId: Union[DatasetLogicalID, DashboardLogicalID, PersonLogicalID, GroupID]

    def __str__(self) -> str:
        json = encode_canonical_json(self.logicalId.to_dict())
        return f"{self.type.name}~{hashlib.md5(json).hexdigest().upper()}"

    def __hash__(self):
        return hash(str(self))
