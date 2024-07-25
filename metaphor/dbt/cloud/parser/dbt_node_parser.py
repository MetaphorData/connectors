import json
from typing import Any, Dict, List, Optional, Union, cast

from metaphor.common.entity_id import EntityId, parts_to_dataset_entity_id
from metaphor.common.utils import unique_list
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_models import (
    GetJobRunModelsJobModels,
)
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_snapshots import (
    GetJobRunSnapshotsJobSnapshots,
)
from metaphor.dbt.cloud.parser.common import parse_depends_on
from metaphor.dbt.util import (
    build_model_docs_url,
    build_system_tags,
    get_dbt_tags_from_meta,
    get_metaphor_tags_from_meta,
    get_model_name_from_unique_id,
    get_ownerships_from_meta,
    get_snapshot_name_from_unique_id,
    init_dataset,
    init_field,
    init_virtual_view,
)
from metaphor.models.metadata_change_event import (
    AssetStructure,
    ColumnTagAssignment,
    DataPlatform,
    Dataset,
    DbtMacro,
    DbtMaterialization,
    DbtMaterializationType,
    DbtMetadataItem,
    DbtModel,
    EntityUpstream,
    Metric,
    OwnershipAssignment,
    TagAssignment,
    VirtualView,
)

NODE_TYPE = Union[GetJobRunModelsJobModels, GetJobRunSnapshotsJobSnapshots]


class NodeParser:
    def __init__(
        self,
        discovery_api: DiscoveryAPIClient,
        config: DbtCloudConfig,
        platform: DataPlatform,
        account: Optional[str],
        docs_base_url: Optional[str],
        project_explore_url: str,
        datasets: Dict[str, Dataset],
        virtual_views: Dict[str, VirtualView],
        metrics: Dict[str, Metric],
    ) -> None:
        self._discovery_api = discovery_api
        self._platform = platform
        self._account = account
        self._docs_base_url = docs_base_url
        self._project_explore_base_url = project_explore_url
        self._meta_ownerships = config.meta_ownerships
        self._meta_tags = config.meta_tags
        self._meta_key_tags = config.meta_key_tags
        self._datasets = datasets
        self._virtual_views = virtual_views
        self._metrics = metrics

        self._env_file_path: Dict[int, str] = dict()

    @staticmethod
    def get_node_name(node: NODE_TYPE):
        if isinstance(node, GetJobRunModelsJobModels):
            return get_model_name_from_unique_id
        return get_snapshot_name_from_unique_id

    def _parse_model_meta(
        self, model: GetJobRunModelsJobModels, virtual_view: VirtualView
    ) -> None:
        if model.materialized_type is None or model.materialized_type.upper() in [
            "EPHEMERAL",
            "OTHER",
        ]:
            return

        if not model.meta:
            return

        # v3 use 'model.config.meta' while v1, v2 use 'model.meta'
        table = model.alias or model.name
        if not model.database or not model.schema_ or not table:
            return

        dataset = init_dataset(
            self._datasets,
            model.database,
            model.schema_,
            table,
            self._platform,
            self._account,
            model.unique_id,
        )

        # Assign ownership & tags to materialized table/view
        ownerships = get_ownerships_from_meta(model.meta, self._meta_ownerships)
        if len(ownerships.materialized_table) > 0:
            dataset.ownership_assignment = OwnershipAssignment(
                ownerships=ownerships.materialized_table
            )
        if len(ownerships.dbt_model) > 0:
            virtual_view.ownership_assignment = OwnershipAssignment(
                ownerships=ownerships.dbt_model
            )

        tag_names = get_metaphor_tags_from_meta(model.meta, self._meta_tags)
        if len(tag_names) > 0:
            dataset.tag_assignment = TagAssignment(tag_names=tag_names)

        # Capture the whole "meta" field as key-value pairs
        if len(model.meta) > 0:
            assert virtual_view.dbt_model
            virtual_view.dbt_model.meta = [
                DbtMetadataItem(key=key, value=json.dumps(value))
                for key, value in cast(Dict[str, Any], model.meta).items()
            ]

    def _parse_model_materialization(
        self, node: GetJobRunModelsJobModels, dbt_model: DbtModel
    ) -> None:
        materialized = node.materialized_type
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
            return
        if node.columns is not None:
            for col in node.columns:
                if not col.name:
                    continue
                column_name = col.name.lower()
                field = init_field(dbt_model.fields, column_name)
                field.description = col.description
                field.native_type = col.type or "Not Set"
                field.tags = col.tags

                if col.meta is not None:
                    self._parse_column_meta(node, column_name, col.meta)

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
        virtual_view.dbt_model = DbtModel(
            package_name=node.package_name,
            description=node.description or None,
            url=f"{self._project_explore_base_url}/{node.unique_id}",
            docs_url=build_model_docs_url(self._docs_base_url, node.unique_id),
            fields=[],
        )
        return virtual_view.dbt_model

    def _set_system_tags(self, node: NODE_TYPE, virtual_view: VirtualView):
        # Treat dbt tags as system tags
        tags: List[str] = unique_list(
            get_dbt_tags_from_meta(node.meta, self._meta_key_tags)
            + (node.tags if node.tags else [])
        )

        if len(tags) > 0:
            virtual_view.system_tags = build_system_tags(tags)

    def _set_entity_upstream(self, virtual_view: VirtualView, dbt_model: DbtModel):
        source_entities = []
        if dbt_model.source_datasets is not None:
            source_entities.extend(dbt_model.source_datasets)
        if dbt_model.source_models is not None:
            source_entities.extend(dbt_model.source_models)
        if len(source_entities) > 0:
            virtual_view.entity_upstream = EntityUpstream(
                source_entities=source_entities,
            )

    def parse(
        self,
        node: NODE_TYPE,
        source_map: Dict[str, EntityId],
        macro_map: Dict[str, DbtMacro],
    ):
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

        # raw_sql & complied_sql got renamed to raw_code & complied_code in V7
        dbt_model.raw_sql = node.raw_code or node.raw_sql
        dbt_model.compiled_sql = node.compiled_code or node.compiled_sql

        if isinstance(node, GetJobRunModelsJobModels):
            self._parse_model_meta(node, virtual_view)
            self._parse_model_materialization(node, dbt_model)
            parse_depends_on(
                self._virtual_views, node.depends_on, source_map, macro_map, dbt_model
            )

        self._parse_node_columns(node, dbt_model)
        self._set_entity_upstream(virtual_view, dbt_model)
