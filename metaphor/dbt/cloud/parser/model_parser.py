import json
from collections import defaultdict
from typing import Any, Dict, List, Mapping, Optional, Set, Union, cast

from metaphor.common.entity_id import EntityId, parts_to_dataset_entity_id
from metaphor.common.logger import get_logger
from metaphor.common.utils import dedup_lists, unique_list
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.discovery_api.generated.get_models import (
    GetModelsEnvironmentAppliedModelsEdgesNode,
    GetModelsEnvironmentAppliedModelsEdgesNodeTests,
)
from metaphor.dbt.cloud.discovery_api.generated.get_snapshots import (
    GetSnapshotsEnvironmentAppliedSnapshotsEdgesNode,
    GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeTests,
)
from metaphor.dbt.cloud.parser.common import (
    DISCOVERY_API_PAGE_LIMIT,
    update_entity_ownership_assignments,
    update_entity_system_tags,
    update_entity_tag_assignments,
)
from metaphor.dbt.util import (
    add_data_quality_monitor,
    find_dataset_by_parts,
    get_dbt_tags_from_meta,
    get_metaphor_tags_from_meta,
    get_model_name_from_unique_id,
    get_ownerships_from_meta,
    get_snapshot_name_from_unique_id,
    init_dataset,
    init_field,
    init_virtual_view,
    parse_date_time_from_result,
)
from metaphor.models.metadata_change_event import (
    AssetStructure,
    ColumnTagAssignment,
    DataMonitorStatus,
    DataPlatform,
    Dataset,
    DbtMaterialization,
    DbtMaterializationType,
    DbtMetadataItem,
    DbtModel,
    DbtTest,
    TagAssignment,
    VirtualView,
)

logger = get_logger()

NODE_TYPE = Union[
    GetModelsEnvironmentAppliedModelsEdgesNode,
    GetSnapshotsEnvironmentAppliedSnapshotsEdgesNode,
]
TEST_NODE = Union[
    GetModelsEnvironmentAppliedModelsEdgesNodeTests,
    GetSnapshotsEnvironmentAppliedSnapshotsEdgesNodeTests,
]


dbt_run_result_output_data_monitor_status_map: Dict[str, DataMonitorStatus] = {
    "warn": DataMonitorStatus.WARNING,
    "skipped": DataMonitorStatus.UNKNOWN,
    "error": DataMonitorStatus.ERROR,
    "fail": DataMonitorStatus.ERROR,
    "runtime error": DataMonitorStatus.ERROR,
    "pass": DataMonitorStatus.PASSED,
    "success": DataMonitorStatus.PASSED,
}


class ModelParser:
    def __init__(
        self,
        discovery_api: DiscoveryAPIClient,
        config: DbtCloudConfig,
        platform: DataPlatform,
        account: Optional[str],
        project_explore_url: str,
        datasets: Dict[str, Dataset],
        virtual_views: Dict[str, VirtualView],
    ) -> None:
        self._discovery_api = discovery_api
        self._platform = platform
        self._account = account
        self._project_explore_base_url = project_explore_url
        self._meta_ownerships = config.meta_ownerships
        self._meta_tags = config.meta_tags
        self._meta_key_tags = config.meta_key_tags
        self._datasets = datasets
        self._virtual_views = virtual_views

    @staticmethod
    def get_node_name(node: NODE_TYPE):
        if isinstance(node, GetModelsEnvironmentAppliedModelsEdgesNode):
            return get_model_name_from_unique_id
        return get_snapshot_name_from_unique_id

    def _parse_model_meta(self, model: NODE_TYPE, virtual_view: VirtualView) -> None:
        if not model.meta:
            return

        table = model.alias or model.name
        if not model.database or not model.schema_ or not table:
            return

        # Assign ownership & tags to materialized table/view
        ownerships = get_ownerships_from_meta(model.meta, self._meta_ownerships)
        tag_names = get_metaphor_tags_from_meta(model.meta, self._meta_tags)
        update_entity_ownership_assignments(virtual_view, ownerships.dbt_model)

        # Only init the dataset if there's ownership or tag names
        if ownerships.materialized_table or tag_names:
            dataset = init_dataset(
                self._datasets,
                model.database,
                model.schema_,
                table,
                self._platform,
                self._account,
                model.unique_id,
            )

            update_entity_ownership_assignments(dataset, ownerships.materialized_table)
            update_entity_tag_assignments(dataset, tag_names)

        # Capture the whole "meta" field as key-value pairs
        if len(model.meta) > 0:
            assert virtual_view.dbt_model

            # Dedups the meta items - if a key appears multiple times with different values
            # we store them as separate metadata items. If there is any duplicate, it is
            # discarded.
            metas: Mapping[str, Set[Any]] = defaultdict(set)
            for meta in virtual_view.dbt_model.meta or []:
                assert meta.key
                metas[meta.key].add(meta.value)
            for key, value in cast(Dict[str, Any], model.meta).items():
                metas[key].add(json.dumps(value))

            virtual_view.dbt_model.meta = [
                DbtMetadataItem(key, value)
                for key, values in metas.items()
                for value in values
            ]

    def _parse_model_materialization(
        self, node: NODE_TYPE, dbt_model: DbtModel
    ) -> None:
        materialized = (
            node.materialized_type
            if isinstance(node, GetModelsEnvironmentAppliedModelsEdgesNode)
            else "SNAPSHOT"
        )
        if materialized is None:
            return

        try:
            materialization_type = DbtMaterializationType[materialized.upper()]
        except KeyError:
            materialization_type = DbtMaterializationType.OTHER

        dbt_model.materialization = DbtMaterialization(
            type=materialization_type,
            target_dataset=str(self._get_node_entity_id(node)),
        )

    def _get_node_entity_id(self, node: NODE_TYPE) -> EntityId:
        return parts_to_dataset_entity_id(
            self._platform,
            self._account,
            node.database,
            node.schema_,
            node.alias or node.name,
        )

    def _parse_node_columns(self, node: NODE_TYPE, dbt_model: DbtModel) -> None:
        if dbt_model.fields is None:
            dbt_model.fields = []
        if not node.catalog or not node.catalog.columns:
            return

        for col in node.catalog.columns:
            if not col.name:
                continue
            field = init_field(dbt_model.fields, col.name)
            field.description = col.description or field.description
            field.native_type = col.type or field.native_type or "Not Set"
            field.tags = dedup_lists(field.tags, col.tags)

            if col.meta is not None:
                self._parse_column_meta(node, col.name.lower(), col.meta)

    def _parse_column_meta(self, node: NODE_TYPE, column_name: str, meta: Dict) -> None:
        table = node.alias or node.name
        if not node.database or not node.schema_ or not table:
            return

        tag_names = get_metaphor_tags_from_meta(meta, self._meta_tags)
        if len(tag_names) == 0:
            return

        dataset = init_dataset(
            self._datasets,
            node.database,
            node.schema_,
            table,
            self._platform,
            self._account,
            node.unique_id,
        )
        if dataset.tag_assignment is None:
            dataset.tag_assignment = TagAssignment()

        if dataset.tag_assignment.column_tag_assignments is None:
            dataset.tag_assignment.column_tag_assignments = []

        dataset.tag_assignment.column_tag_assignments.append(
            ColumnTagAssignment(
                column_name=column_name,
                tag_names=tag_names,
            )
        )

    def _init_dbt_model(self, node: NODE_TYPE, virtual_view: VirtualView):
        if not virtual_view.dbt_model:
            virtual_view.dbt_model = DbtModel(
                package_name=node.package_name,
                description=node.description or None,
                url=f"{self._project_explore_base_url}/{node.unique_id}",
                fields=[],
            )
        return virtual_view.dbt_model

    def _set_system_tags(self, node: NODE_TYPE, virtual_view: VirtualView):
        # Treat dbt tags as system tags
        tags: List[str] = unique_list(
            get_dbt_tags_from_meta(node.meta, self._meta_key_tags)
            + (node.tags if node.tags else [])
        )
        update_entity_system_tags(virtual_view, tags)

    def _parse_test(self, model: DbtModel, test: TEST_NODE) -> None:
        """
        Parse a test node
        """
        dbt_test = DbtTest(
            name=test.name,
            unique_id=test.unique_id,
            columns=[test.column_name] if test.column_name else [],
            # TODO: parse test lineage from macros and set `depends_on_macros` field
        )

        # TODO: add test compiled code from `applied/tests` not from `applied/models/tests`

        if model.tests is None:
            model.tests = []
        model.tests.append(dbt_test)

    def _parse_test_execution_result(
        self,
        model: NODE_TYPE,
        test: TEST_NODE,
    ) -> None:
        """
        Parse a test execution result and update the dataset's data quality monitors
        """
        model_name = model.alias or model.name
        if model.database is None or model.schema_ is None or model_name is None:
            logger.debug(
                f"Skipping model without dataset {model.unique_id} for test {test.unique_id}"
            )
            return

        if (
            not test.name
            or not test.execution_info
            or not test.execution_info.last_run_status
            or not test.execution_info.execute_completed_at
        ):
            logger.warning(
                f"Skipping test {test.unique_id} without name or execution info"
            )
            return

        dataset = find_dataset_by_parts(
            self._datasets,
            self._platform,
            self._account,
            model.database,
            model.schema_,
            model_name,
        )
        if dataset is None:
            logger.warning(
                f"Dataset {model.database}.{model.schema_}.{model_name} doesn't exist, skip updating data quality for test {test.unique_id}"
            )
            return

        status = dbt_run_result_output_data_monitor_status_map[
            test.execution_info.last_run_status
        ]
        last_run = parse_date_time_from_result(test.execution_info.execute_completed_at)
        add_data_quality_monitor(dataset, test.name, test.column_name, status, last_run)

    def _parse_model(
        self,
        node: NODE_TYPE,
    ):
        """
        Parse a model or snapshot node
        """
        node_name_getter = self.get_node_name(node)
        virtual_view = init_virtual_view(
            self._virtual_views, node.unique_id, node_name_getter
        )

        # Extract project directory from the model's unique id
        # Split by ".", and ditch the model name
        directory = node_name_getter(node.unique_id).rsplit(".")[0]
        virtual_view.structure = AssetStructure(
            directories=[directory],
            name=node.name,
        )

        dbt_model = self._init_dbt_model(node, virtual_view)
        self._set_system_tags(node, virtual_view)

        # raw_sql & compiled_sql got renamed to raw_code & complied_code in V7
        dbt_model.raw_sql = node.raw_code
        dbt_model.compiled_sql = node.compiled_code

        self._parse_node_columns(node, dbt_model)
        self._parse_model_materialization(node, dbt_model)
        self._parse_model_meta(node, virtual_view)

        for test in node.tests or []:
            self._parse_test(dbt_model, test)
            self._parse_test_execution_result(node, test)

    def _get_models_in_environment(
        self, environment_id: int
    ) -> List[GetModelsEnvironmentAppliedModelsEdgesNode]:
        """
        Fetch models in a given environment
        """
        model_nodes: List[GetModelsEnvironmentAppliedModelsEdgesNode] = []
        after = None

        while True:
            result = self._discovery_api.get_models(
                environment_id, first=DISCOVERY_API_PAGE_LIMIT, after=after
            )
            applied = result.environment.applied
            if not applied or not applied.models:
                break
            model_nodes += [e.node for e in applied.models.edges]
            after = applied.models.page_info.end_cursor
            if not applied.models.page_info.has_next_page:
                break

        return model_nodes

    def _get_snapshots_in_environment(
        self, environment_id: int
    ) -> List[GetSnapshotsEnvironmentAppliedSnapshotsEdgesNode]:
        """
        Fetch snapshots in a given environment
        """
        snapshot_nodes: List[GetSnapshotsEnvironmentAppliedSnapshotsEdgesNode] = []
        after = None

        while True:
            result = self._discovery_api.get_snapshots(
                environment_id, first=DISCOVERY_API_PAGE_LIMIT, after=after
            )
            applied = result.environment.applied
            if not applied or not applied.snapshots:
                break
            snapshot_nodes += [e.node for e in applied.snapshots.edges]
            after = applied.snapshots.page_info.end_cursor
            if not applied.snapshots.page_info.has_next_page:
                break

        return snapshot_nodes

    def parse(self, environment_id: int) -> None:
        """
        Fetch and parse models and snapshots in a given environment
        """
        models = self._get_models_in_environment(environment_id)
        for model in models:
            self._parse_model(model)

        snapshots = self._get_snapshots_in_environment(environment_id)
        for snapshot in snapshots:
            self._parse_model(snapshot)
