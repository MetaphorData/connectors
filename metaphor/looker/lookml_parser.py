import glob
import logging
import os
from dataclasses import dataclass
from typing import Dict, Optional, Set, Tuple

try:
    import lkml
    import sql_metadata
except ImportError:
    print("Please install metaphor[looker] extra\n")
    raise

from metaphor.models.metadata_change_event import DataPlatform, DatasetLogicalID

from metaphor.common.entity_id import EntityId, EntityType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class Connection:
    name: str
    platform: DataPlatform
    database: str
    account: Optional[str]
    default_schema: Optional[str]


@dataclass
class Explore:
    name: str
    upstream_datasets: Set[EntityId]
    description: Optional[str]
    label: Optional[str]

    @staticmethod
    def from_dict(
        raw_explore: Dict, raw_views: Dict[str, Dict], connection: Connection
    ):

        explore_name = raw_explore["name"]

        # The underlying view of an explore can be specified via "from" or "view_name".
        # When not specified, it's default to the same name as the explore itself.
        # See https://docs.looker.com/reference/explore-params/from-for-explore
        view_name = raw_explore.get("from", raw_explore.get("view_name", explore_name))
        upstream_datasets = Explore._get_upstream(view_name, raw_views, connection)

        # zero or more joins
        for join in raw_explore.get("joins", []):
            # The actual view name may be overridden via "from".
            # See https://docs.looker.com/reference/explore-params/join
            join_view_name = join.get("from", join["name"])

            upstream_datasets.update(
                Explore._get_upstream(join_view_name, raw_views, connection)
            )

        return Explore(
            name=explore_name,
            description=raw_explore.get("description", None),
            label=raw_explore.get("label", None),
            upstream_datasets=upstream_datasets,
        )

    @staticmethod
    def _to_dataset_id(source_name: str, connection: Connection) -> EntityId:
        parts = source_name.split(".")

        if len(parts) == 1:
            # table
            if connection.default_schema is None:
                raise ValueError(
                    f"Default schema is required for the connection {connection.name}"
                )
            full_name = (
                f"{connection.database}.{connection.default_schema}.{source_name}"
            )
        elif len(parts) == 2:
            # schema.table
            full_name = f"{connection.database}.{source_name}"
        elif len(parts) == 3:
            # db.schema.table
            full_name = source_name
        else:
            raise ValueError(f"Invalid source name {source_name}")

        # Normalize dataset name by lower casing & dropping the quotation marks
        full_name = full_name.replace('"', "").lower()

        return EntityId(
            EntityType.DATASET,
            DatasetLogicalID(
                name=full_name, platform=connection.platform, account=connection.account
            ),
        )

    @staticmethod
    def _get_upstream(
        view_name, raw_views: Dict[str, Dict], connection: Connection
    ) -> Set[EntityId]:
        # The source for a view can be specified via "sql_table_name" or "derived_table".
        # When not specified, it's default to the same name as the view itself.
        # https://docs.looker.com/reference/view-params/sql_table_name-for-view

        raw_view = raw_views.get(view_name)
        if raw_view is None:
            raise ValueError(f"Refer to non-existing view {view_name}")

        if "derived_table" in raw_view:
            sql = raw_view["derived_table"].get("sql")
            if sql is not None:
                return Explore._extract_upstream_from_sql(sql, raw_views, connection)

            # TODO(1329): Add support for native derived tables
            return set()

        source_name = raw_view.get("sql_table_name", view_name)
        return {Explore._to_dataset_id(source_name, connection)}

    @staticmethod
    def _extract_upstream_from_sql(
        sql: str, raw_views: Dict[str, Dict], connection: Connection
    ) -> Set[EntityId]:
        upstream: Set[EntityId] = set()
        try:
            tables = sql_metadata.Parser(sql).tables
        except Exception as e:
            logger.warn(f"Failed to parse SQL:\n{sql}\n\nError:{e}")
            return upstream

        for table in tables:
            if table.endswith(".SQL_TABLE_NAME"):
                # Selecting from another derived table
                # https://docs.looker.com/data-modeling/learning-lookml/sql-and-referring-to-lookml
                view_name = table.split(".")[0]
                upstream.update(Explore._get_upstream(view_name, raw_views, connection))
            else:
                upstream.add(Explore._to_dataset_id(table, connection))

        return upstream


@dataclass
class Model:
    explores: Dict[str, Explore]

    @staticmethod
    def from_dict(
        raw_views: Dict[str, Dict],
        raw_explores: Dict[str, Dict],
        connection: Connection,
    ):
        explores = [
            # Ignore refinements since they don't change the sources
            # See https://docs.looker.com/data-modeling/learning-lookml/refinements
            Explore.from_dict(raw_explore, raw_views, connection)
            for raw_explore in raw_explores.values()
            if not raw_explore["name"].startswith("+")
        ]

        return Model(explores=dict((e.name, e) for e in explores))


def _load_included_file(
    include_path: str, base_dir: str
) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    glob_pattern = f"{base_dir}/{include_path}"
    if not glob_pattern.endswith(".lkml"):
        glob_pattern = glob_pattern + ".lkml"

    raw_views = {}
    raw_explores = {}
    for path in glob.glob(glob_pattern, recursive=True):
        with open(path) as f:
            root = lkml.load(f)
            for view in root.get("views", []):
                raw_views[view["name"]] = view

            for explore in root.get("explores", []):
                raw_explores[explore["name"]] = explore

    return raw_views, raw_explores


def _load_model(
    model_path: str, base_dir: str, connections: Dict[str, Connection]
) -> Model:
    with open(model_path) as f:
        model = lkml.load(f)

    raw_views = {}
    raw_explores = {}

    # Add explores & views defined in included files
    for include_path in model.get("includes", []):
        views, explores = _load_included_file(include_path, base_dir)
        raw_views.update(views)
        raw_explores.update(explores)

    # Add explores & views defined in model
    for explore in model.get("explores", []):
        raw_explores[explore["name"]] = explore

    for view in model.get("views", []):
        raw_views[view["name"]] = view

    connection = connections.get(model.get("connection", ""), None)
    if connection is None:
        raise ValueError(f"Model ${model_path} has an invalid connection")

    return Model.from_dict(raw_views, raw_explores, connection)


def parse_models(base_dir: str, connections: Dict[str, Connection]) -> Dict[str, Model]:

    model_map = {}
    for model_path in glob.glob(f"{base_dir}/**/*.model.lkml", recursive=True):
        model_name = os.path.basename(model_path)[0 : -len(".model.lkml")]
        model_map[model_name] = _load_model(model_path, base_dir, connections)

    return model_map
