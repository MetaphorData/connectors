import re
from datetime import datetime, timezone
from email.utils import parseaddr
from pathlib import Path
from typing import Dict, List, Optional, Union

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetDocumentation,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStatistics,
    DbtMacro,
    DbtMacroArgument,
    DbtMaterialization,
    DbtMaterializationType,
    DbtModel,
    DbtTest,
    EntityType,
    FieldDocumentation,
    MetadataChangeEvent,
    PersonLogicalID,
    SchemaField,
    SchemaType,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)

from metaphor.common.entity_id import EntityId, to_virtual_view_entity_id
from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.dbt.config import DbtRunConfig

from .generated.dbt_catalog_v1 import CatalogTable, DbtCatalog
from .generated.dbt_manifest_v3 import (
    CompiledModelNode,
    CompiledSchemaTestNode,
    DbtManifest,
    ParsedMacro,
    ParsedModelNode,
    ParsedSchemaTestNode,
    ParsedSourceDefinition,
)

logger = get_logger(__name__)


# compiled node has 'compiled_sql' field
MODEL_NODE_TYPE = Union[CompiledModelNode, ParsedModelNode]
TEST_NODE_TYPE = Union[CompiledSchemaTestNode, ParsedSchemaTestNode]


class DbtExtractor(BaseExtractor):
    """
    dbt metadata extractor
    Using manifest v3 and catalog v1 schema, but backward-compatible with older schema versions
    """

    @staticmethod
    def config_class():
        return DbtRunConfig

    def __init__(self):
        self.platform: DataPlatform = DataPlatform.UNKNOWN
        self.account: Optional[str] = None
        self.docs_base_url: Optional[str] = None
        self.project_source_url: Optional[str] = None
        self._manifest: DbtManifest
        self._catalog: Optional[DbtCatalog] = None
        self._datasets: Dict[str, Dataset] = {}
        self._virtual_views: Dict[str, VirtualView] = {}

    async def extract(self, config: DbtRunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, DbtExtractor.config_class())

        logger.info("Fetching metadata from DBT repo")
        self.account = config.account
        self.docs_base_url = config.docs_base_url
        self.project_source_url = config.project_source_url

        try:
            self._manifest = DbtManifest.parse_file(Path(config.manifest))
        except Exception as e:
            logger.error(f"Read manifest json error: {e}")
            raise e

        if config.catalog:
            try:
                self._catalog = DbtCatalog.parse_file(Path(config.catalog))
            except Exception as e:
                logger.error(f"Read catalog json error: {e}")
                raise e

        self._parse_manifest()

        self._parse_catalog()

        dataset_events = [
            EventUtil.build_dataset_event(d) for d in self._datasets.values()
        ]
        virtual_view_events = [
            EventUtil.build_virtual_view_event(d) for d in self._virtual_views.values()
        ]
        return dataset_events + virtual_view_events

    def _parse_manifest(self) -> None:
        assert self._manifest is not None

        metadata = self._manifest.metadata

        assert metadata.adapter_type is not None
        platform = metadata.adapter_type.upper()
        assert platform in DataPlatform.__members__, f"Invalid data platform {platform}"
        self.platform = DataPlatform[platform]

        nodes = self._manifest.nodes
        sources = self._manifest.sources
        macros = self._manifest.macros

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

    def _parse_catalog(self) -> None:
        if self._catalog is None:
            return

        for node in self._catalog.nodes.values():
            self._parse_catalog_model(node)

        for source in self._catalog.sources.values():
            self._parse_catalog_source(source)

    def _parse_catalog_model(self, model: CatalogTable) -> None:
        assert model.unique_id is not None

        # Catalog nodes can be either models or seeds.
        # The only way to distinguish them is by their ID prefix
        if not model.unique_id.startswith("model."):
            return

        virtual_view = self._init_virtual_view(model.unique_id)
        dbt_model = virtual_view.dbt_model

        dbt_model.description = dbt_model.description or model.metadata.comment
        dbt_model.docs_url = self._build_docs_url(model.unique_id)

        for col in model.columns.values():
            column_name = col.name.lower()
            field = self._init_field(dbt_model.fields, column_name)
            field.description = field.description or col.comment
            field.native_type = field.native_type or col.type or "Not Set"

    def _build_docs_url(self, unique_id: str) -> Optional[str]:
        return (
            f"{self.docs_base_url}/#!/model/{unique_id}" if self.docs_base_url else None
        )

    def _build_source_code_url(self, file_path: str) -> Optional[str]:
        return (
            f"{self.project_source_url}/{file_path}"
            if self.project_source_url
            else None
        )

    def _parse_catalog_source(self, model: CatalogTable) -> None:
        meta = model.metadata
        columns = model.columns

        assert model.unique_id is not None
        assert meta.database is not None

        dataset = self._init_dataset(
            meta.database, meta.schema_, meta.name, model.unique_id
        )

        # TODO (ch1236): Re-enable once we figure the source & expected format
        # self._init_ownership(dataset)
        # assert dataset.ownership is not None and dataset.ownership.people is not None
        # dataset.ownership.people.append(self._build_owner(meta["owner"]))

        self._init_documentation(dataset)
        if meta.comment:
            dataset.documentation.dataset_documentations = [meta.comment]

        for col in columns.values():
            if col.comment:
                column_name = col.name.lower()
                field_doc = self._init_field_doc(dataset, column_name)
                field_doc.documentation = col.comment

    @staticmethod
    def _parse_catalog_statistics(dataset: Dataset, model: CatalogTable) -> None:
        stats = model.stats

        has_stats = stats.get("has_stats")
        if has_stats is not None and has_stats.value is not None:
            statistics = DatasetStatistics()
            found_statistics = False

            row_count = stats.get("row_count")
            if row_count is not None and row_count.value is not None:
                found_statistics = True
                statistics.record_count = float(row_count.value)

            bytes = stats.get("bytes")
            if bytes is not None and bytes.value is not None:
                found_statistics = True
                statistics.data_size = (
                    float(bytes.value) / 1048576  # convert bytes to MB
                )

            last_modified = stats.get("last_modified")
            if last_modified is not None and last_modified.value is not None:
                found_statistics = True
                if isinstance(last_modified.value, str):
                    # Must set tzinfo explicitly due to https://bugs.python.org/issue22377
                    statistics.last_updated = datetime.strptime(
                        last_modified.value, "%Y-%m-%d %H:%M%Z"
                    ).replace(tzinfo=timezone.utc)
                else:
                    statistics.last_updated = datetime.fromtimestamp(
                        last_modified.value
                    ).replace(tzinfo=timezone.utc)

            if found_statistics:
                dataset.statistics = statistics

    def _parse_manifest_nodes(
        self,
        sources: Dict[str, ParsedSourceDefinition],
        macros: Dict[str, ParsedMacro],
        tests: Dict[str, TEST_NODE_TYPE],
        models: Dict[str, MODEL_NODE_TYPE],
    ) -> None:
        source_map = {}
        for key, source in sources.items():
            assert source.database is not None
            source_map[key] = self._get_dataset_entity_id(
                source.database, source.schema_, source.name
            )

        macro_map = {}
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

        for _, model in models.items():
            self._init_virtual_view(model.unique_id)

        for key, model in models.items():
            virtual_view = self._init_virtual_view(model.unique_id)
            virtual_view.dbt_model = DbtModel(
                package_name=model.package_name,
                description=model.description,
                url=self._build_source_code_url(model.original_file_path),
                tags=model.tags,
                raw_sql=model.raw_sql,
                fields=[],
            )
            dbt_model = virtual_view.dbt_model

            if isinstance(model, CompiledModelNode):
                dbt_model.compiled_sql = model.compiled_sql

            dbt_model.owners = self._get_model_owner_ids(model)

            assert model.config is not None and model.database is not None
            materialized = model.config.materialized

            if materialized:
                try:
                    materialization_type = DbtMaterializationType[materialized.upper()]
                except KeyError:
                    materialization_type = DbtMaterializationType.OTHER

                dbt_model.materialization = DbtMaterialization(
                    type=materialization_type,
                    target_dataset=str(
                        self._get_dataset_entity_id(
                            model.database, model.schema_, model.name
                        )
                    ),
                )

            if model.columns is not None:
                for col in model.columns.values():
                    column_name = col.name.lower()
                    field = self._init_field(dbt_model.fields, column_name)
                    field.description = col.description
                    field.native_type = col.data_type or "Not Set"

            if model.depends_on is not None:
                if model.depends_on.nodes:
                    dbt_model.source_models = [
                        self._get_virtual_view_id(self._virtual_views[n].logical_id)
                        for n in model.depends_on.nodes
                        if n.startswith("model.")
                    ]

                    dbt_model.source_datasets = [
                        str(source_map[n])
                        for n in model.depends_on.nodes
                        if n.startswith("source.")
                    ]

                if model.depends_on.macros:
                    dbt_model.macros = [macro_map[n] for n in model.depends_on.macros]

        for key, test in tests.items():
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

            self._init_dbt_tests(model_unique_id).append(dbt_test)

    def _get_dataset_logical_id(
        self, db: str, schema: str, table: str
    ) -> DatasetLogicalID:
        full_name = f"{db}.{schema}.{table}".lower()

        return DatasetLogicalID(
            name=full_name,
            platform=self.platform,
            account=self.account,
        )

    def _get_dataset_entity_id(self, db: str, schema: str, table: str) -> EntityId:
        return EntityId(
            EntityType.DATASET, self._get_dataset_logical_id(db, schema, table)
        )

    @staticmethod
    def _get_virtual_view_id(logical_id: VirtualViewLogicalID) -> str:
        return str(to_virtual_view_entity_id(logical_id.name, logical_id.type))

    @staticmethod
    def _get_model_name_from_unique_id(unique_id: str) -> str:
        assert unique_id.startswith("model."), f"invalid model id {unique_id}"
        return unique_id[6:]

    @staticmethod
    def _get_model_owner_ids(
        model: MODEL_NODE_TYPE, owner_key="owner"
    ) -> Optional[List[str]]:
        # v3 use 'model.config.meta' while v1, v2 use 'model.meta'
        if model.config and model.config.meta and owner_key in model.config.meta:
            owners = model.config.meta[owner_key]
        elif model.meta and owner_key in model.meta:
            owners = model.meta[owner_key]
        else:
            return None

        parts = re.split(r"(\s|,)", owners.strip())
        return [
            str(EntityId(EntityType.PERSON, PersonLogicalID(email=p)))
            for p in parts
            if "@" in parseaddr(p)[1]
        ]

    def _init_dataset(
        self, database: str, schema: str, name: str, unique_id: str
    ) -> Dataset:
        if unique_id not in self._datasets:
            self._datasets[unique_id] = Dataset(
                logical_id=self._get_dataset_logical_id(database, schema, name)
            )
        return self._datasets[unique_id]

    def _init_virtual_view(self, unique_id: str) -> VirtualView:
        if unique_id not in self._virtual_views:
            self._virtual_views[unique_id] = VirtualView(
                logical_id=VirtualViewLogicalID(
                    name=self._get_model_name_from_unique_id(unique_id),
                    type=VirtualViewType.DBT_MODEL,
                ),
            )
        return self._virtual_views[unique_id]

    def _init_dbt_tests(self, dbt_model_unique_id: str) -> List[DbtTest]:
        assert dbt_model_unique_id in self._virtual_views

        dbt_model = self._virtual_views[dbt_model_unique_id].dbt_model
        if dbt_model.tests is None:
            dbt_model.tests = []
        return dbt_model.tests

    @staticmethod
    def _init_schema(dataset: Dataset) -> None:
        if not dataset.schema:
            dataset.schema = DatasetSchema()
            dataset.schema.schema_type = SchemaType.SQL
            dataset.schema.fields = []

    @staticmethod
    def _init_field(fields: List[SchemaField], column: str) -> SchemaField:
        field = next((f for f in fields if f.field_path == column), None)
        if not field:
            field = SchemaField(field_path=column)
            fields.append(field)
        return field

    @staticmethod
    def _init_documentation(dataset: Dataset) -> None:
        if not dataset.documentation:
            dataset.documentation = DatasetDocumentation()
            dataset.documentation.dataset_documentations = []
            dataset.documentation.field_documentations = []

    @staticmethod
    def _init_field_doc(dataset: Dataset, column: str) -> FieldDocumentation:
        assert (
            dataset.documentation is not None
            and dataset.documentation.field_documentations is not None
        )

        doc = next(
            (
                d
                for d in dataset.documentation.field_documentations
                if d.field_path == column
            ),
            None,
        )
        if not doc:
            doc = FieldDocumentation()
            doc.field_path = column
            dataset.documentation.field_documentations.append(doc)
        return doc
