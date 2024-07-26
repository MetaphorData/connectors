from typing import Dict, List, Union

from metaphor.common.entity_id import EntityId
from metaphor.common.utils import unique_list
from metaphor.dbt.util import get_virtual_view_id
from metaphor.models.metadata_change_event import (
    Dataset,
    DbtMacro,
    DbtMetric,
    DbtModel,
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

    datasets, models, macros = None, None, None

    datasets = unique_list(
        [str(source_map[n]) for n in depends_on if n.startswith("source.")]
    )

    models = unique_list(
        [
            get_virtual_view_id(virtual_views[n].logical_id)  # type: ignore
            for n in depends_on
            if n.startswith("model.") or n.startswith("snapshot.")
        ]
    )

    macros = [
        macro_map[n] for n in depends_on if n.startswith("macro.") and n in macro_map
    ]

    target.source_datasets = datasets if datasets else None
    target.source_models = models if models else None
    if isinstance(target, DbtModel):
        target.macros = macros if macros else None


def dataset_has_parsed_fields(
    dataset: Dataset,
) -> bool:
    """
    init_dataset may generate irrelevant datasets, need to filter these out
    """
    return (
        dataset.ownership_assignment is not None
        or dataset.tag_assignment is not None
        or dataset.documentation is not None
        or dataset.data_quality is not None
    )
