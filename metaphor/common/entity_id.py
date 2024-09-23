from typing import Optional, Union

from canonicaljson import encode_canonical_json
from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.utils import md5_digest
from metaphor.models.logical_id import PersonLogicalID
from metaphor.models.metadata_change_event import (
    DashboardLogicalID,
    DataPlatform,
    DatasetLogicalID,
    EntityType,
    KnowledgeCardLogicalID,
    PipelineLogicalID,
    PipelineType,
    VirtualViewLogicalID,
    VirtualViewType,
)


@dataclass(config=ConnectorConfig)
class EntityId:
    type: EntityType
    logicalId: Union[
        DatasetLogicalID,
        DashboardLogicalID,
        KnowledgeCardLogicalID,
        PersonLogicalID,
        PipelineLogicalID,
        VirtualViewLogicalID,
    ]

    def __str__(self) -> str:
        json = encode_canonical_json(EventUtil.clean_nones(self.logicalId.to_dict()))
        return f"{self.type.name}~{md5_digest(json).upper()}"

    def __hash__(self):
        return hash(str(self))


def to_dataset_entity_id(
    normalized_name: str, platform: DataPlatform, account: Optional[str] = None
) -> EntityId:
    """
    converts a dataset name, platform and account into a dataset entity ID
    """
    return EntityId(
        EntityType.DATASET,
        DatasetLogicalID(name=normalized_name, platform=platform, account=account),
    )


def to_dataset_entity_id_from_logical_id(logical_id: DatasetLogicalID) -> EntityId:
    """
    converts a dataset logical ID to entity ID
    """
    return EntityId(EntityType.DATASET, logical_id)


def to_pipeline_entity_id_from_logical_id(logical_id: PipelineLogicalID) -> EntityId:
    """
    converts a pipeline logical ID to entity ID
    """
    return EntityId(EntityType.PIPELINE, logical_id)


def to_virtual_view_entity_id(name: str, virtualViewType: VirtualViewType) -> EntityId:
    """
    converts a virtual view name and type into a Virtual View entity ID
    """
    return EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name=name, type=virtualViewType),
    )


def to_person_entity_id(email: str) -> EntityId:
    """
    converts an email to a person entity ID
    """
    return EntityId(
        EntityType.PERSON,
        PersonLogicalID(email=email.lower()),
    )


def to_pipeline_entity_id(name: str, pipeline_type: PipelineType) -> EntityId:
    """
    converts a pipeline name and type into a Pipeline entity ID
    """
    return EntityId(
        EntityType.PIPELINE,
        PipelineLogicalID(name=name, type=pipeline_type),
    )


def to_dashboard_entity_id_from_logical_id(logical_id: DashboardLogicalID) -> EntityId:
    """
    converts a dashboard logical ID to entity ID
    """
    return EntityId(EntityType.DASHBOARD, logical_id)


def to_entity_id_from_virtual_view_logical_id(
    logical_id: VirtualViewLogicalID,
) -> EntityId:
    """
    converts a VirtualView logical ID to entity ID
    """
    return EntityId(EntityType.VIRTUAL_VIEW, logical_id)


def normalize_full_dataset_name(name: str) -> str:
    """
    Normalizes a fully qualified dataset name
    """
    return name.lower().replace("`", "").replace('"', "")


def dataset_normalized_name(
    db: Optional[str] = None,
    schema: Optional[str] = None,
    table: Optional[str] = None,
) -> str:
    """
    Builds a normalized dataset name from database, schema, & table names
    """
    return normalize_full_dataset_name(
        ".".join([part for part in [db, schema, table] if part is not None])
    )


def parts_to_dataset_entity_id(
    platform: DataPlatform,
    account: Optional[str],
    database: Optional[str] = None,
    schema: Optional[str] = None,
    table: Optional[str] = None,
) -> EntityId:
    """
    converts parts of a dataset, its platform and account into a dataset entity ID
    """
    return to_dataset_entity_id(
        dataset_normalized_name(database, schema, table),
        platform,
        account,
    )
