from typing import Dict, Union

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DbtMacro,
    DbtMacroArgument,
    DbtMaterialization,
    DbtMaterializationType,
    DbtModel,
    DbtTest,
    OwnershipAssignment,
    TagAssignment,
    VirtualView,
)
from pydantic.utils import unique_list

from metaphor.common.entity_id import EntityId, dataset_fullname, to_dataset_entity_id
from metaphor.common.logger import get_logger
from metaphor.dbt.config import DbtRunConfig

from .generated.dbt_manifest_v3 import (
    CompiledModelNode,
    CompiledSchemaTestNode,
    DbtManifest,
    ParsedMacro,
    ParsedModelNode,
    ParsedSchemaTestNode,
    ParsedSourceDefinition,
)
from .util import (
    build_docs_url,
    build_source_code_url,
    get_ownerships_from_meta,
    get_tags_from_meta,
    get_virtual_view_id,
    init_dataset,
    init_dbt_tests,
    init_documentation,
    init_field,
    init_field_doc,
    init_virtual_view,
)

logger = get_logger(__name__)

# compiled node has 'compiled_sql' field
MODEL_NODE_TYPE = Union[CompiledModelNode, ParsedModelNode]
TEST_NODE_TYPE = Union[CompiledSchemaTestNode, ParsedSchemaTestNode]


class ManifestParserV3:
    """
    dbt manifest parser, using v3 schema https://schemas.getdbt.com/dbt/manifest/v3.json
    backward compatible with older schema
    """

    def __init__(
        self,
        config: DbtRunConfig,
        platform: DataPlatform,
        datasets: Dict[str, Dataset],
        virtual_views: Dict[str, VirtualView],
    ):
        self._platform = platform
        self._account = config.account
        self._docs_base_url = config.docs_base_url
        self._project_source_url = config.project_source_url
        self._meta_ownerships = config.meta_ownerships
        self._meta_tags = config.meta_tags
        self._datasets = datasets
        self._virtual_views = virtual_views

    def parse(self, manifest_json: dict) -> None:
        try:
            manifest = DbtManifest.parse_obj(manifest_json)
        except Exception as e:
            logger.error(f"Parse manifest json error: {e}")
            raise e

        nodes = manifest.nodes
        sources = manifest.sources
        macros = manifest.macros

        models = {
            k: v
            for (k, v) in nodes.items()
            if isinstance(v, (CompiledModelNode, ParsedModelNode))
            # if upgraded to python 3.8+, can use get_args(MODEL_NODE_TYPE)
        }
        tests = {
            k: v
            for (k, v) in nodes.items()
            if isinstance(v, (CompiledSchemaTestNode, ParsedSchemaTestNode))
        }

        self._parse_manifest_nodes(sources, macros, tests, models)

    def _parse_manifest_nodes(
        self,
        sources: Dict[str, ParsedSourceDefinition],
        macros: Dict[str, ParsedMacro],
        tests: Dict[str, TEST_NODE_TYPE],
        models: Dict[str, MODEL_NODE_TYPE],
    ) -> None:
        source_map: Dict[str, EntityId] = {}
        for key, source in sources.items():
            assert source.database is not None
            source_map[key] = to_dataset_entity_id(
                dataset_fullname(source.database, source.schema_, source.identifier),
                self._platform,
                self._account,
            )

            self._parse_source(source)

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

        # initialize all virtual views to be used in cross-references
        for _, model in models.items():
            init_virtual_view(self._virtual_views, model.unique_id)

        for _, model in models.items():
            self._parse_model(model, source_map, macro_map)

        for _, test in tests.items():
            # check test is referring a model
            if test.depends_on is None or not test.depends_on.nodes:
                continue

            model_unique_id = test.depends_on.nodes[0]
            if not model_unique_id.startswith("model."):
                continue

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

            if isinstance(test, CompiledSchemaTestNode):
                dbt_test.sql = test.compiled_sql

            init_dbt_tests(self._virtual_views, model_unique_id).append(dbt_test)

    def _parse_model(
        self,
        model: MODEL_NODE_TYPE,
        source_map: Dict[str, EntityId],
        macro_map: Dict[str, DbtMacro],
    ):
        if model.config is None or model.database is None:
            logger.warn("Skipping model without config or database")
            return

        virtual_view = init_virtual_view(self._virtual_views, model.unique_id)
        virtual_view.dbt_model = DbtModel(
            package_name=model.package_name,
            description=model.description or None,
            url=build_source_code_url(
                self._project_source_url, model.original_file_path
            ),
            docs_url=build_docs_url(self._docs_base_url, model.unique_id),
            tags=model.tags,
            raw_sql=model.raw_sql,
            fields=[],
        )
        dbt_model = virtual_view.dbt_model

        if isinstance(model, CompiledModelNode):
            dbt_model.compiled_sql = model.compiled_sql

        # v3 use 'model.config.meta' while v1, v2 use 'model.meta'
        meta = model.config.meta if model.config.meta else model.meta
        if meta:
            self._parse_model_meta(meta, model)

        materialized = model.config.materialized
        if materialized:
            try:
                materialization_type = DbtMaterializationType[materialized.upper()]
            except KeyError:
                materialization_type = DbtMaterializationType.OTHER

            dbt_model.materialization = DbtMaterialization(
                type=materialization_type,
                target_dataset=str(
                    to_dataset_entity_id(
                        dataset_fullname(
                            model.database, model.schema_, model.alias or model.name
                        ),
                        self._platform,
                        self._account,
                    )
                ),
            )

        if model.columns is not None:
            for col in model.columns.values():
                column_name = col.name.lower()
                field = init_field(dbt_model.fields, column_name)
                field.description = col.description
                field.native_type = col.data_type or "Not Set"

        if model.depends_on is not None:
            if model.depends_on.nodes:
                dbt_model.source_models = unique_list(
                    [
                        get_virtual_view_id(self._virtual_views[n].logical_id)
                        for n in model.depends_on.nodes
                        if n.startswith("model.")
                    ]
                )

                dbt_model.source_datasets = unique_list(
                    [
                        str(source_map[n])
                        for n in model.depends_on.nodes
                        if n.startswith("source.")
                    ]
                )

            if model.depends_on.macros:
                dbt_model.macros = [macro_map[n] for n in model.depends_on.macros]

    def _parse_model_meta(self, meta: Dict, model: MODEL_NODE_TYPE) -> None:
        if (
            not model.config
            or not model.config.materialized
            or model.config.materialized.upper() in ["EPHEMERAL", "OTHER"]
        ):
            return

        def get_dataset():
            return init_dataset(
                self._datasets,
                model.database,
                model.schema_,
                model.alias or model.name,
                self._platform,
                self._account,
                model.unique_id,
            )

        # Assign ownership & tags to materialized table/view
        ownerships = get_ownerships_from_meta(meta, self._meta_ownerships)
        if len(ownerships) > 0:
            get_dataset().ownership_assignment = OwnershipAssignment(
                ownerships=ownerships
            )

        tag_names = get_tags_from_meta(meta, self._meta_tags)
        if len(tag_names) > 0:
            get_dataset().tag_assignment = TagAssignment(tag_names=tag_names)

    def _parse_source(self, source: ParsedSourceDefinition) -> None:
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
