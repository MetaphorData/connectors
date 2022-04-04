import functools
import glob
import logging
import operator
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from metaphor.looker.config import LookerConnectionConfig

try:
    import lkml
    import sql_metadata
except ImportError:
    print("Please install metaphor[looker] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    LookerExplore,
    LookerExploreJoin,
    LookerView,
    LookerViewDimension,
    LookerViewMeasure,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)

from metaphor.common.entity_id import (
    EntityId,
    to_dataset_entity_id,
    to_virtual_view_entity_id,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
class Model:
    explores: Dict[str, Explore]

    @staticmethod
    def from_dict(
        raw_explores: Dict[str, Dict],
    ):
        explores = [
            # Ignore refinements since they don't change the sources
            # See https://docs.looker.com/data-modeling/learning-lookml/refinements
            Explore.from_dict(raw_explore)
            for raw_explore in raw_explores.values()
            if not raw_explore["name"].startswith("+")
        ]

        return Model(explores=dict((e.name, e) for e in explores))


def _to_dataset_id(source_name: str, connection: LookerConnectionConfig) -> EntityId:
    parts = source_name.split(".")

    if len(parts) == 1:
        # table
        if connection.default_schema is None:
            raise ValueError(
                f"Default schema is required for the connection {connection.name}"
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


def _get_extended_view_names(raw_view: Dict) -> Optional[List[str]]:
    # Depending on the version of lkml, the "extends" field can be mapped to either "extends" or "extends__all"
    extends = raw_view.get("extends", raw_view.get("extends__all", None))
    if extends is None:
        return None

    # Flatten the nested list into a list of base view names
    return functools.reduce(operator.concat, extends)


def _sql_table_name_from_extended_views(
    extended_view_names: List[str], raw_views: Dict[str, Dict]
) -> Optional[str]:

    sql_table_name = None
    for extended_view_name in extended_view_names:
        extended_view = raw_views.get(extended_view_name, None)
        assert (
            extended_view is not None
        ), f"Extending non-existing view {extended_view_name}"

        # It's possible to chain extends
        # https://docs.looker.com/data-modeling/learning-lookml/extends#chaining_together_multiple_extends
        chained_extended_view_names = _get_extended_view_names(extended_view)

        # Priority is given to views in the order they're added to the list.
        # https://docs.looker.com/data-modeling/learning-lookml/extends#extending_more_than_one_object_at_the_same_time
        if "sql_table_name" in extended_view:
            sql_table_name = extended_view.get("sql_table_name")
        elif chained_extended_view_names is not None:
            sql_table_name = _sql_table_name_from_extended_views(
                chained_extended_view_names, raw_views
            )

    return sql_table_name


def _get_upstream_datasets(
    view_name, raw_views: Dict[str, Dict], connection: LookerConnectionConfig
) -> Set[EntityId]:

    raw_view = raw_views.get(view_name)
    if raw_view is None:
        logger.error(f"Refer to non-existing view {view_name}")
        return set()

    if "upstream_dataset_ids" in raw_view:
        return raw_view["upstream_dataset_ids"]

    upstreams = set()

    # Inheriting the upstream from extended views
    # https://docs.looker.com/reference/view-params/extends-for-view
    extended_view_names = _get_extended_view_names(raw_view)
    if extended_view_names is not None:
        sql_table_name = _sql_table_name_from_extended_views(
            extended_view_names, raw_views
        )
        if sql_table_name is not None:
            upstreams = set([_to_dataset_id(sql_table_name, connection)])

    # Set/override upstream via derived_table
    # https://docs.looker.com/reference/view-params/derived_table
    derived_table = raw_view.get("derived_table", None)
    if derived_table is not None:
        if "sql" in derived_table:
            upstreams = set(
                _extract_upstream_datasets_from_sql(
                    derived_table["sql"], raw_views, connection
                )
            )

        if "explore_source" in derived_table:
            upstreams = set(
                _get_upstream_datasets(
                    derived_table["explore_source"]["name"], raw_views, connection
                )
            )

    # Set/override upstream via sql_table_name
    # https://docs.looker.com/reference/view-params/sql_table_name-for-view
    sql_table_name = raw_view.get("sql_table_name", None)
    if sql_table_name is not None:
        upstreams = set([_to_dataset_id(sql_table_name, connection)])

    # If none of the above was triggered, assume upstream is a table with the same name
    # https://docs.looker.com/reference/view-params/sql_table_name-for-view
    if len(upstreams) == 0:
        upstreams.add(_to_dataset_id(view_name, connection))

    raw_view["upstream_dataset_ids"] = upstreams
    return upstreams


def _extract_upstream_datasets_from_sql(
    sql: str, raw_views: Dict[str, Dict], connection: LookerConnectionConfig
) -> Set[EntityId]:
    upstreams: Set[EntityId] = set()
    try:
        # strip the brackets around referenced view name
        sql = re.sub(r"\${(.+\.SQL_TABLE_NAME)}", r"\1", sql)

        # parse SQL tables
        tables = sql_metadata.Parser(sql).tables
        for table in tables:
            if table.endswith(".SQL_TABLE_NAME"):
                # Selecting from another derived table
                # https://docs.looker.com/data-modeling/learning-lookml/sql-and-referring-to-lookml
                view_name = table.split(".")[0]
                upstreams.update(
                    _get_upstream_datasets(view_name, raw_views, connection)
                )
            else:
                upstreams.add(_to_dataset_id(table, connection))
    except Exception as e:
        logger.warning(f"Failed to parse SQL:\n{sql}\n\nError:{e}")

    return upstreams


def fullname(model: str, name: str) -> str:
    return f"{model}.{name}"


def _build_looker_view(
    model: str,
    raw_view: Dict,
    raw_views: Dict[str, Dict],
    connection: LookerConnectionConfig,
    url: Optional[str],
) -> VirtualView:
    name = raw_view["name"]
    view = LookerView(
        label=raw_view.get("label"),
        url=url,
    )

    try:
        view.source_datasets = [
            str(ds) for ds in _get_upstream_datasets(name, raw_views, connection)
        ]
    except Exception as e:
        logger.error(f"Can't determine upstream datasets for view {name}")
        logger.exception(e)

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
    )


def _build_looker_explore(
    model: str, raw_explore: Dict, url: Optional[str]
) -> VirtualView:
    name = raw_explore["name"]
    base_view_name = raw_explore.get("from", raw_explore.get("view_name", name))

    explore = LookerExplore(
        model_name=model,
        description=raw_explore.get("description"),
        label=raw_explore.get("label"),
        tags=raw_explore.get("tags"),
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

    return VirtualView(
        logical_id=VirtualViewLogicalID(
            name=fullname(model, name), type=VirtualViewType.LOOKER_EXPLORE
        ),
        looker_explore=explore,
    )


def _get_entity_url(
    path: str, base_dir: str, projectSourceUrl: Optional[str]
) -> Optional[str]:
    if not projectSourceUrl:
        return None

    relative_path = os.path.relpath(path, base_dir)
    return f"{projectSourceUrl}/{relative_path}"


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

    for path in glob.glob(glob_pattern, recursive=True):
        # Skip processed files to avoid circular includes
        normpath = os.path.normpath(path)
        if normpath in processed_files:
            continue
        processed_files.add(normpath)

        url = _get_entity_url(path, base_dir, projectSourceUrl)
        with open(path) as f:
            logger.info(f"Processing view file {normpath}")

            root = lkml.load(f)
            for view in root.get("views", []):
                raw_views[view["name"]] = view
                entity_urls[view["name"]] = url

            for explore in root.get("explores", []):
                raw_explores[explore["name"]] = explore
                entity_urls[explore["name"]] = url

            # A file can further include other files
            # https://docs.looker.com/reference/model-params/include#using_include_in_a_view_file
            for include_path in root.get("includes", []):

                # Include using relative path
                if not include_path.startswith("/"):
                    rel_dir = os.path.relpath(os.path.dirname(path), base_dir)
                    include_path = f"/{rel_dir}/{include_path}"

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
) -> Tuple[
    Dict[str, Dict], Dict[str, Dict], Dict[str, Optional[str]], LookerConnectionConfig
]:
    """
    Loads model file and extract raw Views and Explores
    """
    with open(model_path) as f:
        model = lkml.load(f)

    logger.info(f"Processing model {model_path}")

    raw_views: Dict[str, Dict] = {}
    raw_explores: Dict[str, Dict] = {}
    entity_urls: Dict[str, Optional[str]] = {}

    # Add explores & views defined in included files
    # https://docs.looker.com/reference/model-params/include#using_include_in_a_model_file
    for include_path in model.get("includes", []):
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

    return raw_views, raw_explores, entity_urls, connection


def parse_project(
    base_dir: str,
    connections: Dict[str, LookerConnectionConfig],
    projectSourceUrl: Optional[str] = None,
) -> Tuple[Dict[str, Model], List[VirtualView]]:
    """
    parse the project under base_dir, returning a Model map and a list of virtual views including
    Looker Explores and Views
    https://docs.looker.com/data-modeling/getting-started/how-project-works
    """
    model_map = {}
    virtual_views = []

    for model_path in glob.glob(f"{base_dir}/**/*.model.lkml", recursive=True):
        model_name = os.path.basename(model_path)[0 : -len(".model.lkml")]
        raw_views, raw_explores, entity_urls, connection = _load_model(
            model_path, base_dir, connections, projectSourceUrl
        )

        virtual_views.extend(
            [
                _build_looker_view(
                    model_name,
                    view,
                    raw_views,
                    connection,
                    entity_urls.get(view["name"]),
                )
                for view in raw_views.values()
                # Exclude views that require extension
                # https://docs.looker.com/reference/view-params/extension-for-view
                if view.get("extension", "") != "required"
            ]
        )

        virtual_views.extend(
            [
                _build_looker_explore(
                    model_name, explore, entity_urls.get(explore["name"])
                )
                for explore in raw_explores.values()
            ]
        )

        model_map[model_name] = Model.from_dict(raw_explores)

    return model_map, virtual_views
