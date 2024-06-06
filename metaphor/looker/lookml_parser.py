import functools
import glob
import logging
import operator
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

try:
    import lkml
except ImportError:
    print("Please install metaphor[looker] extra\n")
    raise

from metaphor.common.entity_id import (
    EntityId,
    to_dataset_entity_id,
    to_virtual_view_entity_id,
)
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.utils import unique_list
from metaphor.looker.config import LookerConnectionConfig
from metaphor.models.metadata_change_event import (
    AssetStructure,
    EntityType,
    EntityUpstream,
    LookerExplore,
    LookerExploreJoin,
    LookerView,
    LookerViewDimension,
    LookerViewMeasure,
    LookerViewQuery,
    SystemTag,
    SystemTags,
    SystemTagSource,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)

logger = get_logger()

# lkml parser's debug log can be very noisy
logging.getLogger("lkml.parser").setLevel(logging.WARNING)


@dataclass
class Explore:
    name: str

    @staticmethod
    def from_dict(
        raw_explore: Dict,
    ):
        return Explore(
            name=raw_explore["name"],
        )


@dataclass
class RawModel:
    raw_views: Dict[str, Dict]
    raw_explores: Dict[str, Dict]


@dataclass
class Model:
    explores: Dict[str, Explore]

    @staticmethod
    def from_dict(
        raw_model: RawModel,
    ):
        explores = [
            # Ignore refinements since they don't change the sources
            # See https://docs.looker.com/data-modeling/learning-lookml/refinements
            Explore.from_dict(raw_explore)
            for raw_explore in raw_model.raw_explores.values()
            if not raw_explore["name"].startswith("+")
        ]

        return Model(explores=dict((e.name, e) for e in explores))


ModelMap = Dict[str, Model]


_parsed_files: Dict[str, Dict] = {}


def _parse_lookml_file(path, base_dir) -> Dict:

    if path in _parsed_files:
        return _parsed_files[path]

    with open(path) as f:
        model = lkml.load(f)
        _parsed_files[path] = model

        sanitized = os.path.relpath(path, base_dir).replace("/", "__")
        json_dump_to_debug_file(model, f"{sanitized}.json")
        return model


def _to_dataset_id(source_name: str, connection: LookerConnectionConfig) -> EntityId:
    parts = source_name.split(".")

    if len(parts) == 1:
        # table
        if connection.default_schema is None:
            raise ValueError(
                f"Default schema is required for the connection {connection.database}"
            )
        full_name = f"{connection.database}.{connection.default_schema}.{source_name}"
    elif len(parts) == 2:
        # schema.table
        full_name = f"{connection.database}.{source_name}"
    elif len(parts) == 3:
        # db.schema.table
        full_name = source_name
    else:
        raise ValueError(f"Invalid source name {source_name}")

    # Normalize dataset name by lower casing & dropping the quotation marks
    full_name = full_name.replace('"', "").replace("`", "").lower()

    return to_dataset_entity_id(full_name, connection.platform, connection.account)


def _get_upstream_and_query(
    model_name: str,
    view_name: str,
    raw_model: RawModel,
    connection: LookerConnectionConfig,
) -> Tuple[Set[EntityId], Optional[LookerViewQuery]]:
    raw_views = raw_model.raw_views
    raw_view = raw_views.get(view_name)
    if raw_view is None:
        logger.error(f"Refer to non-existing view {view_name}")
        return set(), None

    if "upstream_entity_ids" in raw_view:
        return raw_view["upstream_entity_ids"], raw_view["query"]

    upstreams = set()  # upstream dataset id or virtual view id
    query = None

    # Set upstream via derived_table
    # https://docs.looker.com/reference/view-params/derived_table
    derived_table: Optional[Dict] = raw_view.get("derived_table")
    if derived_table is not None:
        if "sql" in derived_table:
            query = LookerViewQuery(
                query=derived_table["sql"],
                source_platform=connection.platform,
                source_dataset_account=connection.account,
                default_database=connection.database,
                default_schema=connection.default_schema,
            )

        # https://docs.looker.com/data-modeling/learning-lookml/creating-ndts
        if "explore_source" in derived_table:
            explore_name = derived_table["explore_source"]["name"]
            raw_explore = raw_model.raw_explores.get(explore_name)
            assert raw_explore is not None, f"Invalid explore_source: {explore_name}"

            base_view_name = _get_base_view_name(raw_explore, raw_model)
            if base_view_name is None:
                base_view_name = explore_name

            assert base_view_name in raw_views, f"Invalid base view {base_view_name}"

            # TODO: handle field mapping

            upstreams.add(
                to_virtual_view_entity_id(
                    fullname(model_name, base_view_name), VirtualViewType.LOOKER_VIEW
                )
            )

    # Set upstream via sql_table_name
    # https://docs.looker.com/reference/view-params/sql_table_name-for-view
    sql_table_name: Optional[str] = raw_view.get("sql_table_name")
    if sql_table_name is not None:
        upstreams = {_to_dataset_id(sql_table_name, connection)}

    # If none of the above was triggered, assume upstream is a table with the same name
    # https://docs.looker.com/reference/view-params/sql_table_name-for-view
    if query is None and len(upstreams) == 0:
        upstreams.add(_to_dataset_id(view_name, connection))

    raw_view["upstream_entity_ids"] = upstreams
    raw_view["query"] = query
    return upstreams, query


def fullname(model: str, name: str) -> str:
    return f"{model}.{name}"


def _get_model_asset_structure(model: str, name: str) -> AssetStructure:
    return AssetStructure(
        directories=[model],
        name=name,
    )


def _build_looker_view(
    model: str,
    raw_view: Dict,
    raw_model: RawModel,
    connection: LookerConnectionConfig,
    url: Optional[str],
) -> VirtualView:
    name = raw_view["name"]
    view = LookerView(
        label=raw_view.get("label"),
        url=url,
    )

    source_entities = []
    try:
        upstream_entity_ids, query = _get_upstream_and_query(
            model, name, raw_model, connection
        )
        if upstream_entity_ids:
            source_entities = [str(entity_id) for entity_id in upstream_entity_ids]
            view.source_datasets = [
                str(entity_id)
                for entity_id in upstream_entity_ids
                if entity_id.type == EntityType.DATASET
            ] or None
        if query is not None:
            view.query = query
    except Exception:
        logger.exception(f"Can't determine upstream for view {name}")

    if "dimensions" in raw_view:
        view.dimensions = [
            LookerViewDimension(
                field=raw_dimension["name"],
                data_type=raw_dimension.get("type", raw_dimension["name"]),
            )
            for raw_dimension in raw_view["dimensions"]
        ]

    if "measures" in raw_view:
        view.measures = [
            LookerViewMeasure(
                field=raw_measure["name"],
                type=raw_measure.get("type", "N/A"),
            )
            for raw_measure in raw_view["measures"]
        ]

    return VirtualView(
        logical_id=VirtualViewLogicalID(
            name=fullname(model, name), type=VirtualViewType.LOOKER_VIEW
        ),
        looker_view=view,
        structure=_get_model_asset_structure(model, name),
        entity_upstream=(
            EntityUpstream(source_entities=source_entities) if source_entities else None
        ),
    )


def _get_base_view_name(raw_explore: Dict, raw_model: RawModel) -> str:
    # https://docs.looker.com/reference/explore-params/from-for-explore
    if "from" in raw_explore:
        return raw_explore["from"]

    # https://docs.looker.com/reference/explore-params/view_name
    if "view_name" in raw_explore:
        return raw_explore["view_name"]

    # Default to the view with the same name if nothing is specified
    return raw_explore["name"]


def _build_looker_explore(
    model: str, raw_explore: Dict, raw_model: RawModel, url: Optional[str]
) -> VirtualView:
    name = raw_explore["name"]
    base_view_name = _get_base_view_name(raw_explore, raw_model)
    tag_names: List[str] = raw_explore.get("tags", [])

    explore = LookerExplore(
        model_name=model,
        description=raw_explore.get("description"),
        label=raw_explore.get("label"),
        url=url,
        fields=raw_explore.get("fields"),
        base_view=str(
            to_virtual_view_entity_id(
                fullname(model, base_view_name), VirtualViewType.LOOKER_VIEW
            )
        ),
    )

    if "extends" in raw_explore:
        explore.extends = [
            str(
                to_virtual_view_entity_id(
                    fullname(model, explore), VirtualViewType.LOOKER_EXPLORE
                )
            )
            for explore in raw_explore["extends"]
        ]

    if "joins" in raw_explore:
        explore.joins = [
            LookerExploreJoin(
                view=str(
                    to_virtual_view_entity_id(
                        fullname(model, raw_join.get("from", raw_join["name"])),
                        VirtualViewType.LOOKER_VIEW,
                    )
                ),
                fields=raw_join.get("fields"),
                on_clause=raw_join.get("sql_on"),
                where_clause=raw_join.get("sql_where"),
                type=raw_join.get("type", "left_outer"),
                relationship=raw_join.get("relationship", "many_to_one"),
            )
            for raw_join in raw_explore["joins"]
        ]

    # TODO: combine access_filters, always_filters and conditional_filters into explore.filters

    # combine base view and joined view into source entities
    source_entities = unique_list(
        [explore.base_view] + [join.view for join in explore.joins or []]
    )

    tags = [
        SystemTag(key=None, system_tag_source=SystemTagSource.LOOKER, value=value)
        for value in tag_names
    ]
    return VirtualView(
        logical_id=VirtualViewLogicalID(
            name=fullname(model, name), type=VirtualViewType.LOOKER_EXPLORE
        ),
        looker_explore=explore,
        structure=_get_model_asset_structure(model, name),
        entity_upstream=EntityUpstream(source_entities=source_entities),
        system_tags=SystemTags(tags=tags),
    )


def _get_extended_names(raw_view_or_explore: Dict) -> Optional[List[str]]:
    # Depending on the version of lkml, the "extends" field can be mapped to either "extends" or "extends__all"
    extends = raw_view_or_explore.get(
        "extends", raw_view_or_explore.get("extends__all", None)
    )
    if extends is None:
        return None

    # Flatten the nested list into a list of names
    return functools.reduce(operator.concat, extends)


def _extend_view(raw_view: Dict, raw_views: Dict[str, Dict]) -> Dict:
    """Extends a view by merging parameters from extended views

    See https://docs.looker.com/reference/view-params/extends-for-view
    """

    def merge_views(base: Dict, extension: Dict) -> Dict:
        # Conflict resolution in LookML extensions is quite complicated.
        # We're only approximating the behavior here for the metadata we care about.
        # See https://docs.looker.com/data-modeling/learning-lookml/extends#combining_parameters

        merged = dict(base)
        for key, val in extension.items():
            if key == "extension":
                # "extension" should not get merged
                pass
            elif key == "sql_table_name":
                # merging "sql_table_name" should also clear "derived_table"
                merged.pop("derived_table", None)
                merged[key] = val
            elif key == "derived_table":
                # merge "derived_table" only if "sql" or "explore_source" is set in value
                # also clear "sql_table_name"
                if "sql" in val or "explore_source" in val:
                    merged.pop("sql_table_name", None)
                    merged[key] = val
            else:
                merged[key] = val

        return merged

    extended_view_names = _get_extended_names(raw_view)
    if extended_view_names is None:
        return raw_view

    # Priority is given to views based on the order in the list.
    # https://docs.looker.com/data-modeling/learning-lookml/extends#extending_more_than_one_object_at_the_same_time
    merged_view: Dict = {}
    for view_name in extended_view_names:
        extended_view = raw_views.get(view_name, None)
        assert extended_view is not None, f"Extending non-existing view {view_name}"

        # It's possible to chain extends
        # https://docs.looker.com/data-modeling/learning-lookml/extends#chaining_together_multiple_extends
        chained_extended_view = _extend_view(extended_view, raw_views)
        merged_view = merge_views(merged_view, chained_extended_view)

    return merge_views(merged_view, raw_view)


def _extend_explore(raw_explore: Dict, raw_explores: Dict[str, Dict]) -> Dict:
    """Extends a view by merging parameters from extended views

    See https://docs.looker.com/reference/view-params/extends-for-view
    """

    def merge_explores(base: Dict, extension: Dict) -> Dict:
        # Conflict resolution in LookML extensions is quite complicated.
        # We're only approximating the behavior here for the metadata we care about.
        # See https://docs.looker.com/data-modeling/learning-lookml/extends#combining_parameters

        merged = dict(base)
        for key, val in extension.items():
            if key == "extension":
                # "extension" should not get merged
                # https://docs.looker.com/reference/view-params/extension-for-view
                pass
            elif key == "from" or key == "view_name":
                # "from" & "view_name" are mutually exclusive when merging
                merged.pop("view_name", None)
                merged.pop("from", None)
                merged[key] = val
            else:
                merged[key] = val

        return merged

    extended_explore_names = _get_extended_names(raw_explore)
    if extended_explore_names is None:
        return raw_explore

    # Priority is given to explores based on the order in the list.
    # https://docs.looker.com/data-modeling/learning-lookml/extends#extending_more_than_one_object_at_the_same_time
    merged_explore: Dict = {}
    for explore_name in extended_explore_names:
        extended_explore = raw_explores.get(explore_name, None)
        assert extended_explore is not None, f"Extending invalid explore {explore_name}"

        # It's possible to chain extends
        # https://docs.looker.com/data-modeling/learning-lookml/extends#chaining_together_multiple_extends
        chained_extended_explore = _extend_explore(extended_explore, raw_explores)
        merged_explore = merge_explores(merged_explore, chained_extended_explore)

    return merge_explores(merged_explore, raw_explore)


def _resolve_model(raw_model: RawModel) -> RawModel:
    resolved_views = {}
    for name, view in raw_model.raw_views.items():
        resolved_views[name] = _extend_view(view, raw_model.raw_views)

    resolved_explores = {}
    for name, explore in raw_model.raw_explores.items():
        resolved_explores[name] = _extend_explore(explore, raw_model.raw_explores)

    return RawModel(
        raw_views=resolved_views,
        raw_explores=resolved_explores,
    )


def _get_entity_url(
    path: str, base_dir: str, projectSourceUrl: Optional[str]
) -> Optional[str]:
    if not projectSourceUrl:
        return None

    relative_path = os.path.relpath(path, base_dir)
    return f"{projectSourceUrl}/{relative_path}"


def _to_absolute_include(include_path: str, file_path: str, base_dir: str) -> str:
    """Convert a relative include path into an absolute include"""

    if include_path.startswith("/"):
        return include_path

    rel_dir = os.path.relpath(os.path.dirname(file_path), base_dir)
    return f"/{rel_dir}/{include_path}"


def _load_included_file(
    include_path: str,
    base_dir: str,
    projectSourceUrl: Optional[str],
    raw_views: Dict[str, Dict],
    raw_explores: Dict[str, Dict],
    entity_urls: Dict[str, Optional[str]],
    processed_files: Set[str],
):
    """Load all files matched by the pattern defined in include_path

    All parsed views and explores will be added to raw_views & raw_explores, along with a
    corresponding URL in entity_urls. This function will also recursively load any additional
    files includes by each file.
    """
    glob_pattern = f"{base_dir}/{include_path}"
    if not glob_pattern.endswith(".lkml"):
        glob_pattern = glob_pattern + ".lkml"

    for file_path in glob.glob(glob_pattern, recursive=True):
        # Skip processed files to avoid circular includes
        normpath = os.path.normpath(file_path)
        if normpath in processed_files:
            continue
        processed_files.add(normpath)

        url = _get_entity_url(file_path, base_dir, projectSourceUrl)
        logger.info(f"Processing view file {normpath}")

        root = _parse_lookml_file(file_path, base_dir)
        for view in root.get("views", []):
            raw_views[view["name"]] = view
            entity_urls[view["name"]] = url

        for explore in root.get("explores", []):
            raw_explores[explore["name"]] = explore
            entity_urls[explore["name"]] = url

        # A file can further include other files
        # https://docs.looker.com/reference/model-params/include#using_include_in_a_view_file
        for include_path in root.get("includes", []):
            # Convert to absolute include
            include_path = _to_absolute_include(include_path, file_path, base_dir)

            _load_included_file(
                include_path,
                base_dir,
                projectSourceUrl,
                raw_views,
                raw_explores,
                entity_urls,
                processed_files,
            )


def _load_model(
    model_path: str,
    base_dir: str,
    connections: Dict[str, LookerConnectionConfig],
    projectSourceUrl: Optional[str],
) -> Tuple[RawModel, Dict[str, Optional[str]], LookerConnectionConfig]:
    """
    Loads model file and extract raw Views and Explores
    """
    model = _parse_lookml_file(model_path, base_dir)
    logger.info(f"Processing model {model_path}")

    raw_views: Dict[str, Dict] = {}
    raw_explores: Dict[str, Dict] = {}
    entity_urls: Dict[str, Optional[str]] = {}

    # Add explores & views defined in included files
    # https://docs.looker.com/reference/model-params/include#using_include_in_a_model_file
    for include_path in model.get("includes", []):
        # Convert to absolute include
        include_path = _to_absolute_include(include_path, model_path, base_dir)

        _load_included_file(
            include_path,
            base_dir,
            projectSourceUrl,
            raw_views,
            raw_explores,
            entity_urls,
            set(),
        )

    url = _get_entity_url(model_path, base_dir, projectSourceUrl)

    # Add explores & views defined in model
    for explore in model.get("explores", []):
        raw_explores[explore["name"]] = explore
        entity_urls[explore["name"]] = url

    for view in model.get("views", []):
        raw_views[view["name"]] = view
        entity_urls[view["name"]] = url

    connection_name = model.get("connection", "").lower()
    connection = connections.get(connection_name)
    if connection is None:
        raise ValueError(
            f"Model {model_path} has an invalid connection {connection_name}"
        )

    raw_model = RawModel(raw_views=raw_views, raw_explores=raw_explores)

    return raw_model, entity_urls, connection


def parse_project(
    base_dir: str,
    connections: Dict[str, LookerConnectionConfig],
    projectSourceUrl: Optional[str] = None,
) -> Tuple[ModelMap, List[VirtualView]]:
    """
    parse the project under base_dir, returning a Model map and a list of virtual views including
    Looker Explores and Views
    https://docs.looker.com/data-modeling/getting-started/how-project-works
    """
    model_map = {}
    virtual_views = []

    for model_path in glob.glob(f"{base_dir}/**/*.model.lkml", recursive=True):
        model_name = os.path.basename(model_path)[0 : -len(".model.lkml")]
        raw_model, entity_urls, connection = _load_model(
            model_path, base_dir, connections, projectSourceUrl
        )

        resolved_model = _resolve_model(raw_model)

        virtual_views.extend(
            [
                _build_looker_view(
                    model_name,
                    view,
                    resolved_model,
                    connection,
                    entity_urls.get(view["name"]),
                )
                for view in resolved_model.raw_views.values()
                # Exclude views that require extension
                # https://docs.looker.com/reference/view-params/extension-for-view
                if view.get("extension", "") != "required"
            ]
        )

        virtual_views.extend(
            [
                _build_looker_explore(
                    model_name,
                    explore,
                    resolved_model,
                    entity_urls.get(explore["name"]),
                )
                for explore in resolved_model.raw_explores.values()
                # Exclude explores that require extension
                # https://docs.looker.com/reference/view-params/extension-for-view
                if explore.get("extension", "") != "required"
            ]
        )

        model_map[model_name] = Model.from_dict(raw_model)

    return model_map, virtual_views
