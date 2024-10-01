import json
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union

from metaphor.common.entity_id import EntityId
from metaphor.common.logger import get_logger
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.common.utils import filter_none, unique_list
from metaphor.dbt.config import DbtRunConfig
from metaphor.dbt.util import (
    add_data_quality_monitor,
    build_metric_docs_url,
    build_model_docs_url,
    build_source_code_url,
    build_system_tags,
    dataset_normalized_name,
    get_dbt_tags_from_meta,
    get_metaphor_tags_from_meta,
    get_model_name_from_unique_id,
    get_ownerships_from_meta,
    get_snapshot_name_from_unique_id,
    get_virtual_view_id,
    init_dataset,
    init_dbt_tests,
    init_documentation,
    init_field,
    init_field_doc,
    init_metric,
    init_virtual_view,
    to_dataset_entity_id,
)
from metaphor.models.metadata_change_event import (
    AssetStructure,
    ColumnTagAssignment,
    DataMonitorStatus,
    DataPlatform,
    Dataset,
    DbtMacro,
    DbtMacroArgument,
    DbtMaterialization,
    DbtMaterializationType,
    DbtMetadataItem,
    DbtMetric,
    DbtModel,
    DbtTest,
    EntityUpstream,
    Metric,
    MetricFilter,
    OwnershipAssignment,
    TagAssignment,
    VirtualView,
)

from .generated.dbt_manifest_v5 import CompiledGenericTestNode as CompiledTestNodeV5
from .generated.dbt_manifest_v5 import CompiledModelNode as CompiledModelNodeV5
from .generated.dbt_manifest_v5 import CompiledSnapshotNode as CompiledSnapshotNodeV5
from .generated.dbt_manifest_v5 import DbtManifest as DbtManifestV5
from .generated.dbt_manifest_v5 import DependsOn as DependsOnV5
from .generated.dbt_manifest_v5 import ParsedGenericTestNode as ParsedTestNodeV5
from .generated.dbt_manifest_v5 import ParsedMacro as ParsedMacroV5
from .generated.dbt_manifest_v5 import ParsedMetric as ParsedMetricV5
from .generated.dbt_manifest_v5 import ParsedModelNode as ParsedModelNodeV5
from .generated.dbt_manifest_v5 import ParsedSnapshotNode as ParsedSnapshotNodeV5
from .generated.dbt_manifest_v5 import (
    ParsedSourceDefinition as ParsedSourceDefinitionV5,
)
from .generated.dbt_manifest_v6 import CompiledGenericTestNode as CompiledTestNodeV6
from .generated.dbt_manifest_v6 import CompiledModelNode as CompiledModelNodeV6
from .generated.dbt_manifest_v6 import CompiledSnapshotNode as CompiledSnapshotNodeV6
from .generated.dbt_manifest_v6 import DbtManifest as DbtManifestV6
from .generated.dbt_manifest_v6 import DependsOn as DependsOnV6
from .generated.dbt_manifest_v6 import ParsedGenericTestNode as ParsedTestNodeV6
from .generated.dbt_manifest_v6 import ParsedMacro as ParsedMacroV6
from .generated.dbt_manifest_v6 import ParsedMetric as ParsedMetricV6
from .generated.dbt_manifest_v6 import ParsedModelNode as ParsedModelNodeV6
from .generated.dbt_manifest_v6 import ParsedSnapshotNode as ParsedSnapshotNodeV6
from .generated.dbt_manifest_v6 import (
    ParsedSourceDefinition as ParsedSourceDefinitionV6,
)
from .generated.dbt_manifest_v7 import CompiledGenericTestNode as CompiledTestNodeV7
from .generated.dbt_manifest_v7 import CompiledModelNode as CompiledModelNodeV7
from .generated.dbt_manifest_v7 import CompiledSnapshotNode as CompiledSnapshotNodeV7
from .generated.dbt_manifest_v7 import DbtManifest as DbtManifestV7
from .generated.dbt_manifest_v7 import DependsOn as DependsOnV7
from .generated.dbt_manifest_v7 import ParsedGenericTestNode as ParsedTestNodeV7
from .generated.dbt_manifest_v7 import ParsedMacro as ParsedMacroV7
from .generated.dbt_manifest_v7 import ParsedMetric as ParsedMetricV7
from .generated.dbt_manifest_v7 import ParsedModelNode as ParsedModelNodeV7
from .generated.dbt_manifest_v7 import ParsedSnapshotNode as ParsedSnapshotNodeV7
from .generated.dbt_manifest_v7 import (
    ParsedSourceDefinition as ParsedSourceDefinitionV7,
)
from .generated.dbt_manifest_v8 import DbtManifest as DbtManifestV8
from .generated.dbt_manifest_v8 import DependsOn as DependsOnV8
from .generated.dbt_manifest_v8 import GenericTestNode as GenericTestNodeV8
from .generated.dbt_manifest_v8 import Macro as MacroV8
from .generated.dbt_manifest_v8 import Metric as MetricV8
from .generated.dbt_manifest_v8 import ModelNode as ModelNodeV8
from .generated.dbt_manifest_v8 import SnapshotNode as SnapshotNodeV8
from .generated.dbt_manifest_v8 import SourceDefinition as SourceDefinitionV8
from .generated.dbt_manifest_v9 import DbtManifest as DbtManifestV9
from .generated.dbt_manifest_v9 import DependsOn as DependsOnV9
from .generated.dbt_manifest_v9 import GenericTestNode as GenericTestNodeV9
from .generated.dbt_manifest_v9 import Macro as MacroV9
from .generated.dbt_manifest_v9 import Metric as MetricV9
from .generated.dbt_manifest_v9 import ModelNode as ModelNodeV9
from .generated.dbt_manifest_v9 import SnapshotNode as SnapshotNodeV9
from .generated.dbt_manifest_v9 import SourceDefinition as SourceDefinitionV9
from .generated.dbt_manifest_v10 import DbtManifest as DbtManifestV10
from .generated.dbt_manifest_v10 import DependsOn as DependsOnV10
from .generated.dbt_manifest_v10 import GenericTestNode as GenericTestNodeV10
from .generated.dbt_manifest_v10 import Macro as MacroV10
from .generated.dbt_manifest_v10 import Metric as MetricV10
from .generated.dbt_manifest_v10 import ModelNode as ModelNodeV10
from .generated.dbt_manifest_v10 import SnapshotNode as SnapshotNodeV10
from .generated.dbt_manifest_v10 import SourceDefinition as SourceDefinitionV10
from .generated.dbt_manifest_v11 import DependsOn as DependsOnV11
from .generated.dbt_manifest_v11 import GenericTestNode as GenericTestNodeV11
from .generated.dbt_manifest_v11 import Macro as MacroV11
from .generated.dbt_manifest_v11 import Metric as MetricV11
from .generated.dbt_manifest_v11 import ModelNode as ModelNodeV11
from .generated.dbt_manifest_v11 import SnapshotNode as SnapshotNodeV11
from .generated.dbt_manifest_v11 import SourceDefinition as SourceDefinitionV11
from .generated.dbt_manifest_v11 import WritableManifest as DbtManifestV11
from .generated.dbt_manifest_v12 import DependsOn as DependsOnV12
from .generated.dbt_manifest_v12 import GenericTest as GenericTestNodeV12
from .generated.dbt_manifest_v12 import Macro as MacroV12
from .generated.dbt_manifest_v12 import Metric as MetricV12
from .generated.dbt_manifest_v12 import Model as ModelNodeV12
from .generated.dbt_manifest_v12 import Snapshot as SnapshotNodeV12
from .generated.dbt_manifest_v12 import SourceDefinition as SourceDefinitionV12
from .generated.dbt_manifest_v12 import WritableManifest as DbtManifestV12
from .generated.dbt_run_results_v4 import DbtRunResults as DbtRunResultsV4
from .generated.dbt_run_results_v4 import RunResultOutput as RunResultOutputV4
from .generated.dbt_run_results_v5 import RunResultOutput as RunResultOutputV5
from .generated.dbt_run_results_v5 import RunResultsArtifact as DbtRunResultsV5
from .generated.dbt_run_results_v6 import RunResultOutput as RunResultOutputV6
from .generated.dbt_run_results_v6 import RunResultsArtifact as DbtRunResultsV6

logger = get_logger()


MODEL_NODE_TYPE = Union[
    CompiledModelNodeV5,
    CompiledModelNodeV6,
    CompiledModelNodeV7,
    ParsedModelNodeV5,
    ParsedModelNodeV6,
    ParsedModelNodeV7,
    ModelNodeV8,
    ModelNodeV9,
    ModelNodeV10,
    ModelNodeV11,
    ModelNodeV12,
]

TEST_NODE_TYPE = Union[
    CompiledTestNodeV5,
    CompiledTestNodeV6,
    CompiledTestNodeV7,
    GenericTestNodeV8,
    GenericTestNodeV9,
    GenericTestNodeV10,
    GenericTestNodeV11,
    GenericTestNodeV12,
    ParsedTestNodeV5,
    ParsedTestNodeV6,
    ParsedTestNodeV7,
]

SNAPSHOT_NODE_TYPES = [
    CompiledSnapshotNodeV5,
    CompiledSnapshotNodeV6,
    CompiledSnapshotNodeV7,
    ParsedSnapshotNodeV5,
    ParsedSnapshotNodeV6,
    ParsedSnapshotNodeV7,
    SnapshotNodeV8,
    SnapshotNodeV9,
    SnapshotNodeV10,
    SnapshotNodeV11,
    SnapshotNodeV12,
]

SNAPSHOT_NODE_TYPE = Union[
    CompiledSnapshotNodeV5,
    CompiledSnapshotNodeV6,
    CompiledSnapshotNodeV7,
    ParsedSnapshotNodeV5,
    ParsedSnapshotNodeV6,
    ParsedSnapshotNodeV7,
    SnapshotNodeV8,
    SnapshotNodeV9,
    SnapshotNodeV10,
    SnapshotNodeV11,
    SnapshotNodeV12,
]

VIRTUAL_VIEW_NODE_TYPE = Union[
    MODEL_NODE_TYPE,
    SNAPSHOT_NODE_TYPE,
]


def node_name_getter(node: VIRTUAL_VIEW_NODE_TYPE) -> Callable[[str], str]:
    for _type in SNAPSHOT_NODE_TYPES:
        if isinstance(node, _type):
            return get_snapshot_name_from_unique_id
    else:
        return get_model_name_from_unique_id


SOURCE_DEFINITION_TYPE = Union[
    ParsedSourceDefinitionV5,
    ParsedSourceDefinitionV6,
    ParsedSourceDefinitionV7,
    SourceDefinitionV8,
    SourceDefinitionV9,
    SourceDefinitionV10,
    SourceDefinitionV11,
    SourceDefinitionV12,
]

SOURCE_DEFINITION_MAP = Union[
    Dict[str, ParsedSourceDefinitionV5],
    Dict[str, ParsedSourceDefinitionV6],
    Dict[str, ParsedSourceDefinitionV7],
    Dict[str, SourceDefinitionV8],
    Dict[str, SourceDefinitionV9],
    Dict[str, SourceDefinitionV10],
    Dict[str, SourceDefinitionV11],
    Dict[str, SourceDefinitionV12],
]

METRIC_TYPE = Union[
    ParsedMetricV5,
    ParsedMetricV6,
    ParsedMetricV7,
    MetricV8,
    MetricV9,
    MetricV10,
    MetricV11,
    MetricV12,
]

DEPENDS_ON_TYPE = Union[
    DependsOnV5,
    DependsOnV6,
    DependsOnV7,
    DependsOnV8,
    DependsOnV9,
    DependsOnV10,
    DependsOnV11,
    DependsOnV12,
]

MACRO_MAP = Union[
    Dict[str, ParsedMacroV5],
    Dict[str, ParsedMacroV6],
    Dict[str, ParsedMacroV7],
    Dict[str, MacroV8],
    Dict[str, MacroV9],
    Dict[str, MacroV10],
    Dict[str, MacroV11],
    Dict[str, MacroV12],
]

MANIFEST_CLASS_TYPE = Union[
    Type[DbtManifestV5],
    Type[DbtManifestV6],
    Type[DbtManifestV7],
    Type[DbtManifestV8],
    Type[DbtManifestV9],
    Type[DbtManifestV10],
    Type[DbtManifestV11],
    Type[DbtManifestV12],
]

# Maps dbt schema version to manifest class
dbt_version_manifest_class_map: Dict[str, MANIFEST_CLASS_TYPE] = {
    "v4": DbtManifestV5,
    "v5": DbtManifestV5,
    "v6": DbtManifestV6,
    "v7": DbtManifestV7,
    "v8": DbtManifestV8,
    "v9": DbtManifestV9,
    "v10": DbtManifestV10,
    "v11": DbtManifestV11,
    "v12": DbtManifestV12,
}

RUN_RESULTS_TYPE = Union[
    DbtRunResultsV4,
    DbtRunResultsV5,
    DbtRunResultsV6,
]

RUN_RESULTS_CLASS_TYPE = Union[
    Type[DbtRunResultsV4],
    Type[DbtRunResultsV5],
    Type[DbtRunResultsV6],
]

RUN_RESULT_OUTPUT_TYPE = Union[
    RunResultOutputV4,
    RunResultOutputV5,
    RunResultOutputV6,
]

# Maps dbt schema version to run_results class
dbt_version_run_results_class_map: Dict[str, RUN_RESULTS_CLASS_TYPE] = {
    "v4": DbtRunResultsV4,
    "v5": DbtRunResultsV5,
    "v6": DbtRunResultsV6,
}

dbt_run_result_output_data_monitor_status_map: Dict[str, DataMonitorStatus] = {
    "warn": DataMonitorStatus.WARNING,
    "skipped": DataMonitorStatus.UNKNOWN,
    "error": DataMonitorStatus.ERROR,
    "fail": DataMonitorStatus.ERROR,
    "runtime error": DataMonitorStatus.ERROR,
    "pass": DataMonitorStatus.PASSED,
    "success": DataMonitorStatus.PASSED,
}
"""Maps a `RunResultOutput.status` to `DataMonitorStatus`."""


class ArtifactParser:
    def __init__(
        self,
        config: DbtRunConfig,
        platform: DataPlatform,
        datasets: Dict[str, Dataset],
        virtual_views: Dict[str, VirtualView],
        metrics: Dict[str, Metric],
        referenced_virtual_views: Set[str],
    ):
        self._platform = platform
        self._docs_base_url = config.docs_base_url
        self._project_source_url = config.project_source_url
        self._meta_ownerships = config.meta_ownerships
        self._meta_tags = config.meta_tags
        self._meta_key_tags = config.meta_key_tags
        self._datasets = datasets
        self._virtual_views = virtual_views
        self._metrics = metrics
        self._referenced_virtual_views = referenced_virtual_views

        self._account = config.account
        if self._account and platform == DataPlatform.SNOWFLAKE:
            self._account = normalize_snowflake_account(self._account)

    @staticmethod
    def sanitize_manifest(manifest_json: Dict, schema_version: str) -> Dict:
        manifest_json = deepcopy(manifest_json)

        # It's possible for dbt to generate "docs block" in the manifest that doesn't
        # conform to the JSON schema. Specifically, the "name" field can be None in
        # some cases. Since the field is not actually used, it's safe to clear it out to
        # avoid hitting any validation issues.
        if manifest_json.get("docs") is not None:
            manifest_json["docs"] = {}

        # dbt can erroneously generate null for test's "depends_on"
        # Filter these out for now
        nodes = manifest_json.get("nodes", {})
        for key, node in nodes.items():
            if key.startswith("test."):
                depends_on = node.get("depends_on", {})
                depends_on["macros"] = filter_none(depends_on.get("macros", []))
                depends_on["nodes"] = filter_none(depends_on.get("nodes", []))

        # dbt can erroneously set null for metrics' "type_params.conversion_type_params"
        # Filter these out for now
        metrics = manifest_json.get("metrics", {})
        for node in metrics.values():
            node.get("type_params", {}).pop("conversion_type_params", None)

        return manifest_json

    @staticmethod
    def sanitize_run_results(run_results: Dict, schema_version: str) -> Dict:
        run_results = deepcopy(run_results)

        # Temporarily strip off all the extra "compiled", "compiled_code",
        # and "relation_name" fields in results until
        # https://github.com/dbt-labs/dbt-core/issues/8851 is fixed
        for result in run_results.get("results", []):
            result.pop("compiled", None)
            result.pop("compiled_code", None)
            result.pop("relation_name", None)

        return run_results

    def parse(self, manifest_json: Dict, run_results_json: Optional[Dict]) -> None:
        run_results = (
            None
            if run_results_json is None
            else self.parse_run_results(run_results_json)
        )
        self.parse_manifest(manifest_json, run_results)

    def parse_run_results(self, run_results_json: Dict) -> RUN_RESULTS_TYPE:
        metadata = run_results_json.get("metadata", {})

        schema_version = (
            metadata.get("dbt_schema_version", "").rsplit("/", 1)[-1].split(".")[0]
        )
        logger.info(f"parsing run_results.json {schema_version} ...")

        run_results_json = ArtifactParser.sanitize_run_results(
            run_results_json, schema_version
        )

        dbt_run_results_class = dbt_version_run_results_class_map.get(schema_version)
        if dbt_run_results_class is None:
            raise ValueError(f"unsupported run-results schema '{schema_version}'")

        return dbt_run_results_class.model_validate(run_results_json)

    def parse_manifest(
        self, manifest_json: Dict, run_results: Optional[RUN_RESULTS_TYPE]
    ):
        manifest_metadata = manifest_json.get("metadata", {})

        schema_version = (
            manifest_metadata.get("dbt_schema_version", "")
            .rsplit("/", 1)[-1]
            .split(".")[0]
        )
        project_name = manifest_metadata.get("project_name", "")

        logger.info(f"parsing manifest.json {schema_version} ...")

        manifest_json = ArtifactParser.sanitize_manifest(manifest_json, schema_version)

        dbt_manifest_class = dbt_version_manifest_class_map.get(schema_version)
        if dbt_manifest_class is None:
            raise ValueError(f"unsupported manifest schema '{schema_version}'")

        try:
            manifest = dbt_manifest_class.model_validate(manifest_json)
        except Exception as e:
            logger.error(f"Parse manifest json error: {e}")
            raise e

        nodes = manifest.nodes
        sources = manifest.sources
        macros = manifest.macros
        metrics = manifest.metrics

        models = {
            k: v
            for (k, v) in nodes.items()
            if isinstance(
                v,
                (
                    CompiledModelNodeV5,
                    CompiledModelNodeV6,
                    CompiledModelNodeV7,
                    ParsedModelNodeV5,
                    ParsedModelNodeV6,
                    ParsedModelNodeV7,
                    ModelNodeV8,
                    ModelNodeV9,
                    ModelNodeV10,
                    ModelNodeV11,
                    ModelNodeV12,
                ),
            )
            # if upgraded to python 3.8+, can use get_args(MODEL_NODE_TYPE)
        }
        tests = {
            k: v
            for (k, v) in nodes.items()
            if isinstance(
                v,
                (
                    CompiledTestNodeV5,
                    CompiledTestNodeV6,
                    CompiledTestNodeV7,
                    ParsedTestNodeV5,
                    ParsedTestNodeV6,
                    ParsedTestNodeV7,
                    GenericTestNodeV8,
                    GenericTestNodeV9,
                    GenericTestNodeV10,
                    GenericTestNodeV11,
                    GenericTestNodeV12,
                ),
            )
            # if upgraded to python 3.8+, can use get_args(TEST_NODE_TYPE)
        }

        snapshots = {
            k: v
            for (k, v) in nodes.items()
            if isinstance(
                v,
                (
                    CompiledSnapshotNodeV5,
                    CompiledSnapshotNodeV6,
                    CompiledSnapshotNodeV7,
                    ParsedSnapshotNodeV5,
                    ParsedSnapshotNodeV6,
                    ParsedSnapshotNodeV7,
                    SnapshotNodeV8,
                    SnapshotNodeV9,
                    SnapshotNodeV10,
                    SnapshotNodeV11,
                    SnapshotNodeV12,
                ),
            )
            # if upgraded to python 3.8+, can use get_args(SNAPSHOT_NODE_TYPE)
        }

        source_map = self._parse_sources(sources)

        macro_map = self._parse_macros(macros)

        # We generate a virtual view for normal dbt model or snapshot
        virtual_view_nodes: List[VIRTUAL_VIEW_NODE_TYPE] = [
            *models.values(),
            *snapshots.values(),
        ]

        # initialize all virtual views to be used in cross-references
        for node in virtual_view_nodes:
            init_virtual_view(
                self._virtual_views,
                node.unique_id,
                node_name_getter(node),
            )
            if project_name and node.package_name != project_name:
                self._referenced_virtual_views.add(node.unique_id)

        for node in virtual_view_nodes:
            self._parse_virtual_view_node(node, source_map, macro_map)

        for _, test in tests.items():
            self._parse_test(test, run_results, models)

        for _, metric in metrics.items():
            self._parse_metric(metric, source_map, macro_map)

    def _parse_test(
        self,
        test: TEST_NODE_TYPE,
        run_results: Optional[RUN_RESULTS_TYPE],
        models: Dict[str, MODEL_NODE_TYPE],
    ) -> None:
        # check test is referring a model
        if test.depends_on is None or not test.depends_on.nodes:
            return

        model_unique_id = test.depends_on.nodes[0]
        if hasattr(test, "attached_node") and test.attached_node:
            model_unique_id = test.attached_node
        if not model_unique_id.startswith("model."):
            return

        # Skip test if it references an non-existing (most likely disabled) model
        if model_unique_id not in self._virtual_views:
            logger.warning(
                f"Test {test.unique_id} references non-active model {model_unique_id}"
            )
            return

        columns = []
        if test.columns:
            columns = list(test.columns.keys())
        elif test.column_name:
            columns = [test.column_name]

        dbt_test = DbtTest(
            name=test.name,
            unique_id=test.unique_id,
            columns=columns,
            depends_on_macros=test.depends_on.macros,
        )

        # V7 renamed "compiled_sql" to "compiled_code"
        if hasattr(test, "compiled_sql"):
            dbt_test.sql = getattr(test, "compiled_sql")

        if hasattr(test, "compiled_code"):
            dbt_test.sql = getattr(test, "compiled_code")

        init_dbt_tests(self._virtual_views, model_unique_id).append(dbt_test)

        if run_results is not None:
            self._parse_test_run_result(test, models[model_unique_id], run_results)

    def _parse_test_run_result(
        self,
        test: TEST_NODE_TYPE,
        model: MODEL_NODE_TYPE,
        run_results: RUN_RESULTS_TYPE,
    ) -> None:
        if model.config is None or model.database is None:
            logger.warning(
                f"Skipping model without config or database, {model.unique_id}"
            )
            return

        def find_run_result_ouptput_by_id(
            run_results: RUN_RESULTS_TYPE, unique_id: str
        ) -> Optional[RUN_RESULT_OUTPUT_TYPE]:
            return next(
                (
                    run_result
                    for run_result in run_results.results
                    if run_result.unique_id == unique_id
                ),
                None,
            )

        run_result = find_run_result_ouptput_by_id(run_results, test.unique_id)
        if run_result is None:
            logger.warning(f"Cannot find run result for test: {test.unique_id}")
            return

        dataset = init_dataset(
            self._datasets,
            model.database,
            model.schema_,
            model.alias or model.name,
            self._platform,
            self._account,
            model.unique_id,
        )

        status = dbt_run_result_output_data_monitor_status_map[run_result.status]
        last_run = None
        completed_times = sorted(
            timing.completed_at
            for timing in run_result.timing
            if isinstance(timing.completed_at, datetime)
        )
        if completed_times:
            last_run = completed_times[-1]

        add_data_quality_monitor(dataset, test.name, test.column_name, status, last_run)

    def _parse_virtual_view_node(
        self,
        node: VIRTUAL_VIEW_NODE_TYPE,
        source_map: Dict[str, EntityId],
        macro_map: Dict[str, DbtMacro],
    ):
        if node.config is None or node.database is None:
            logger.warning(
                f"Skipping model / snapshot without config or database, {node.unique_id}"
            )
            return

        virtual_view = init_virtual_view(
            self._virtual_views, node.unique_id, node_name_getter(node)
        )

        # Extract project directory from the model's unique id
        # Split by ".", and ditch the model name
        directory = node_name_getter(node)(node.unique_id).rsplit(".")[0]
        virtual_view.structure = AssetStructure(
            directories=[directory],
            name=node.name,
        )

        virtual_view.dbt_model = DbtModel(
            package_name=node.package_name,
            description=node.description or None,
            url=build_source_code_url(
                self._project_source_url, node.original_file_path
            ),
            docs_url=build_model_docs_url(self._docs_base_url, node.unique_id),
            fields=[],
        )
        dbt_model = virtual_view.dbt_model

        # Treat dbt tags as system tags
        tags = get_dbt_tags_from_meta(node.meta, self._meta_key_tags)
        if node.tags:
            tags.extend(node.tags)
            tags = unique_list(tags)

        if len(tags) > 0:
            virtual_view.system_tags = build_system_tags(tags)

        # raw_sql & complied_sql got renamed to raw_code & complied_code in V7
        if hasattr(node, "raw_sql"):
            virtual_view.dbt_model.raw_sql = getattr(node, "raw_sql")
            dbt_model.raw_sql = getattr(node, "raw_sql")

        if hasattr(node, "compiled_sql"):
            virtual_view.dbt_model.compiled_sql = getattr(node, "compiled_sql")
            dbt_model.compiled_sql = getattr(node, "compiled_sql")

        if hasattr(node, "raw_code"):
            virtual_view.dbt_model.raw_sql = getattr(node, "raw_code")
            dbt_model.raw_sql = getattr(node, "raw_code")

        if hasattr(node, "compiled_code"):
            virtual_view.dbt_model.compiled_sql = getattr(node, "compiled_code")
            dbt_model.compiled_sql = getattr(node, "compiled_code")

        self._parse_node_meta(node, virtual_view)

        self._parse_node_materialization(node, dbt_model)

        self._parse_node_columns(node, dbt_model)

        (
            dbt_model.source_datasets,
            dbt_model.source_models,
            dbt_model.macros,
        ) = self._parse_depends_on(node.depends_on, source_map, macro_map)

        source_entities = []
        if dbt_model.source_datasets is not None:
            source_entities.extend(dbt_model.source_datasets)
        if dbt_model.source_models is not None:
            source_entities.extend(dbt_model.source_models)
        if len(source_entities) > 0:
            virtual_view.entity_upstream = EntityUpstream(
                source_entities=source_entities,
            )

    def _parse_macros(self, macros: MACRO_MAP) -> Dict[str, DbtMacro]:
        macro_map: Dict[str, DbtMacro] = {}
        for key, macro in macros.items():
            arguments = (
                [
                    DbtMacroArgument(
                        name=arg.name,
                        type=arg.type,
                        description=arg.description,
                    )
                    for arg in macro.arguments
                ]
                if macro.arguments
                else []
            )

            macro_map[key] = DbtMacro(
                name=macro.name,
                unique_id=macro.unique_id,
                package_name=macro.package_name,
                description=macro.description,
                arguments=arguments,
                sql=macro.macro_sql,
                depends_on_macros=macro.depends_on.macros if macro.depends_on else None,
            )

        return macro_map

    def _parse_node_meta(
        self, node: VIRTUAL_VIEW_NODE_TYPE, virtual_view: VirtualView
    ) -> None:
        if node.config is None:
            return

        if (
            node.config is None
            or node.config.materialized is None
            or node.config.materialized.upper() in ["EPHEMERAL", "OTHER"]
        ):
            return

        # v3 use 'model.config.meta' while v1, v2 use 'model.meta'
        meta: Dict[str, Any] = node.config.meta if node.config.meta else node.meta or {}

        def get_dataset():
            return init_dataset(
                self._datasets,
                node.database,
                node.schema_,
                node.alias or node.name,
                self._platform,
                self._account,
                node.unique_id,
            )

        # Assign ownership & tags to materialized table/view
        ownerships = get_ownerships_from_meta(meta, self._meta_ownerships)
        if len(ownerships.materialized_table) > 0:
            get_dataset().ownership_assignment = OwnershipAssignment(
                ownerships=ownerships.materialized_table
            )
        if len(ownerships.dbt_model) > 0:
            virtual_view.ownership_assignment = OwnershipAssignment(
                ownerships=ownerships.dbt_model
            )

        tag_names = get_metaphor_tags_from_meta(meta, self._meta_tags)
        if len(tag_names) > 0:
            get_dataset().tag_assignment = TagAssignment(tag_names=tag_names)

        # Capture the whole "meta" field as key-value pairs
        if len(meta) > 0:
            virtual_view.dbt_model.meta = [
                DbtMetadataItem(key=key, value=json.dumps(value))
                for key, value in meta.items()
            ]

    def _parse_node_materialization(
        self, node: VIRTUAL_VIEW_NODE_TYPE, dbt_model: DbtModel
    ) -> None:
        if node.config is None:
            return

        materialized = node.config.materialized
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

    def _get_node_entity_id(self, node: VIRTUAL_VIEW_NODE_TYPE) -> EntityId:
        return self._get_dataset_entity_id(
            node.database, node.schema_, node.alias or node.name
        )

    def _get_dataset_entity_id(
        self, database: Optional[str], schema: str, table: str
    ) -> EntityId:
        return to_dataset_entity_id(
            dataset_normalized_name(database, schema, table),
            self._platform,
            self._account,
        )

    def _parse_node_columns(
        self, node: VIRTUAL_VIEW_NODE_TYPE, dbt_model: DbtModel
    ) -> None:
        if node.columns is not None:
            for col in node.columns.values():
                column_name = col.name.lower()
                field = init_field(dbt_model.fields, column_name)
                field.description = col.description
                field.native_type = col.data_type or "Not Set"
                field.tags = col.tags

                if col.meta is not None:
                    self._parse_column_meta(node, column_name, col.meta)

    def _parse_column_meta(
        self, node: VIRTUAL_VIEW_NODE_TYPE, column_name: str, meta: Dict
    ) -> None:
        if node.config is None or node.database is None:
            logger.warning(
                f"Skipping model without config or database, {node.unique_id}"
            )
            return

        tag_names = get_metaphor_tags_from_meta(meta, self._meta_tags)
        if len(tag_names) == 0:
            return

        dataset = init_dataset(
            self._datasets,
            node.database,
            node.schema_,
            node.alias or node.name,
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

    def _parse_sources(self, sources: SOURCE_DEFINITION_MAP) -> Dict[str, EntityId]:
        source_map: Dict[str, EntityId] = {}
        for key, source in sources.items():
            assert source.database is not None
            source_map[key] = self._get_dataset_entity_id(
                source.database, source.schema_, source.identifier
            )

            self._parse_source(source)

        return source_map

    def _parse_source(self, source: SOURCE_DEFINITION_TYPE) -> None:
        if not source.database or not source.columns:
            return

        dataset = init_dataset(
            self._datasets,
            source.database,
            source.schema_,
            source.identifier,
            self._platform,
            self._account,
            source.unique_id,
        )

        init_documentation(dataset)
        if source.description:
            dataset.documentation.dataset_documentations = [source.description]

        for col in source.columns.values():
            if col.description:
                column_name = col.name.lower()
                field_doc = init_field_doc(dataset, column_name)
                field_doc.documentation = col.description

    def _parse_depends_on(
        self,
        depends_on: Optional[DEPENDS_ON_TYPE],
        source_map: Dict[str, EntityId],
        macro_map: Dict[str, DbtMacro],
    ) -> Tuple[Optional[List], Optional[List], Optional[List]]:
        datasets, models, macros = None, None, None
        if depends_on is None:
            return datasets, models, macros

        if depends_on.nodes:
            datasets = unique_list(
                [
                    str(source_map[n])
                    for n in depends_on.nodes
                    if n.startswith("source.")
                ]
            )

            models = unique_list(
                [
                    get_virtual_view_id(self._virtual_views[n].logical_id)
                    for n in depends_on.nodes
                    if n.startswith("model.") or n.startswith("snapshot.")
                ]
            )

        if depends_on.macros:
            macros = [macro_map[n] for n in depends_on.macros]

        return datasets, models, macros

    def _parse_metric(
        self,
        metric: METRIC_TYPE,
        source_map: Dict[str, EntityId],
        macro_map: Dict[str, DbtMacro],
    ) -> None:
        # TODO: Add support for v10+ Metric
        if isinstance(metric, (MetricV10, MetricV11, MetricV12)):
            return

        metric_entity = init_metric(self._metrics, metric.unique_id)
        dbt_metric = DbtMetric(
            package_name=metric.package_name,
            description=metric.description or None,
            label=metric.label,
            timestamp=metric.timestamp,
            time_grains=metric.time_grains,
            dimensions=metric.dimensions,
            filters=[
                MetricFilter(field=f.field, operator=f.operator, value=f.value)
                for f in metric.filters
            ],
            url=build_metric_docs_url(self._docs_base_url, metric.unique_id),
        )
        metric_entity.dbt_metric = dbt_metric
        if metric.tags:
            metric_entity.system_tags = build_system_tags(metric.tags)

        # V7 renamed sql & type to expression & calculation_method
        if hasattr(metric, "sql"):
            metric_entity.dbt_metric.sql = getattr(metric, "sql")

        if hasattr(metric, "type"):
            metric_entity.dbt_metric.type = getattr(metric, "type")

        if hasattr(metric, "expression"):
            metric_entity.dbt_metric.sql = getattr(metric, "expression")

        if hasattr(metric, "calculation_method"):
            metric_entity.dbt_metric.type = getattr(metric, "calculation_method")

        (
            dbt_metric.source_datasets,
            dbt_metric.source_models,
            _,
        ) = self._parse_depends_on(metric.depends_on, source_map, macro_map)

        metric_entity.entity_upstream = EntityUpstream(
            source_entities=dbt_metric.source_models,
        )
