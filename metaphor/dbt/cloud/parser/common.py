import json
from typing import Callable, Dict, List, Union

from metaphor.common.entity_id import EntityId
from metaphor.common.utils import dedup_lists, unique_list
from metaphor.dbt.util import (
    build_system_tags,
    get_model_name_from_unique_id,
    get_snapshot_name_from_unique_id,
    get_virtual_view_id,
    init_virtual_view,
)
from metaphor.models.metadata_change_event import (
    Dataset,
    DbtMacro,
    DbtMetric,
    DbtModel,
    Metric,
    Ownership,
    OwnershipAssignment,
    TagAssignment,
    VirtualView,
)


def parse_depends_on(
    virtual_views: Dict[str, VirtualView],
    depends_on: List[str],
    source_map: Dict[str, EntityId],
    macro_map: Dict[str, DbtMacro],
    target: Union[DbtModel, DbtMetric],
):
    if not depends_on:
        return

    target.source_datasets = (
        unique_list(
            [str(source_map[n]) for n in depends_on if n.startswith("source.")]
            + (target.source_datasets or [])
        )
        or None
    )

    def get_source_model_name(n: str) -> Callable[[str], str]:
        if n.startswith("model."):
            return get_model_name_from_unique_id
        if n.startswith("snapshot."):
            return get_snapshot_name_from_unique_id
        assert False

    target.source_models = (
        unique_list(
            [
                get_virtual_view_id(init_virtual_view(virtual_views, n, get_source_model_name(n)).logical_id)  # type: ignore
                for n in depends_on
                if n.startswith("model.") or n.startswith("snapshot.")
            ]
            + (target.source_models or [])
        )
        or None
    )

    if isinstance(target, DbtModel):
        DbtMacro.__hash__ = lambda macro: hash(json.dumps(macro.to_dict()))  # type: ignore
        target.macros = (
            unique_list(
                [
                    macro_map[n]
                    for n in depends_on
                    if n.startswith("macro.") and n in macro_map
                ]
                + (
                    target.macros
                    if isinstance(target, DbtModel) and target.macros
                    else []
                )
            )
            or None
        )


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
