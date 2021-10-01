import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, cast

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetDocumentation,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStatistics,
    DatasetUpstream,
    EntityType,
    FieldDocumentation,
    MetadataChangeEvent,
    SchemaField,
    SchemaType,
    SQLSchema,
)
from serde import deserialize

from metaphor.common.entity_id import EntityId
from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor, RunConfig

from .generated.dbt_catalog import CatalogTable, DbtCatalog
from .generated.dbt_manifest import CompiledModelNode, DbtManifest, ParsedSchemaTestNode

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class DbtDataset:
    database: str
    schema: str
    name: str


@deserialize
@dataclass
class DbtRunConfig(RunConfig):
    manifest: str
    catalog: str

    # the database service account this DBT project is connected to
    account: Optional[str] = None


class DbtExtractor(BaseExtractor):
    """DBT metadata extractor"""

    @staticmethod
    def config_class():
        return DbtRunConfig

    def __init__(self):
        self.account = None
        self._manifest: DbtManifest
        self._catalog: DbtCatalog
        self._metadata: Dict[str, Dataset] = {}

    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, DbtExtractor.config_class())

        logger.info("Fetching metadata from DBT repo")
        self.account = config.account

        try:
            self._manifest = DbtManifest.parse_file(Path(config.manifest))
        except Exception as e:
            logger.error(f"Read manifest json error: {e}")
            raise e

        try:
            self._catalog = DbtCatalog.parse_file(Path(config.catalog))
        except Exception as e:
            logger.error(f"Read catalog json error: {e}")
            raise e

        platform = self._parse_manifest()
        if platform is None:
            raise ValueError("Invalid platform from manifest.json")

        self._parse_catalog(platform)

        logger.debug(
            json.dumps(
                [EventUtil.clean_nones(d.to_dict()) for d in self._metadata.values()]
            )
        )

        return [EventUtil.build_dataset_event(d) for d in self._metadata.values()]

    def _parse_manifest(self) -> Optional[str]:
        assert self._manifest is not None

        metadata = self._manifest.metadata
        nodes = self._manifest.nodes
        sources = self._manifest.sources

        assert metadata.adapter_type is not None
        platform = metadata.adapter_type.upper()

        models = {
            k: cast(CompiledModelNode, v)
            for (k, v) in nodes.items()
            if isinstance(v, CompiledModelNode)
        }
        tests = {
            k: cast(ParsedSchemaTestNode, v)
            for (k, v) in nodes.items()
            if isinstance(v, ParsedSchemaTestNode)
        }

        dataset_map = {}
        for key, model in models.items():
            assert model.database is not None
            dataset_map[key] = DbtDataset(model.database, model.schema_, model.name)
        for key, source in sources.items():
            assert source.database is not None
            dataset_map[key] = DbtDataset(source.database, source.schema_, source.name)

        for model in models.values():
            self._parse_model(model, platform, dataset_map)

        for test in tests.values():
            self._parse_test(test)

        return platform

    def _parse_model(
        self,
        model: CompiledModelNode,
        platform: str,
        dataset_map: Dict[str, DbtDataset],
    ) -> None:

        assert model.database is not None
        dataset = self._init_dataset(
            model.database, model.schema_, model.name, platform
        )
        self._init_schema(dataset)

        if model.compiled_sql is not None:
            assert dataset.schema is not None
            dataset.schema.sql_schema = SQLSchema()
            dataset.schema.sql_schema.table_schema = model.compiled_sql

        if model.description is not None:
            dataset.schema.description = model.description
            self._init_documentation(dataset)
            assert (
                dataset.documentation is not None
                and dataset.documentation.dataset_documentations is not None
            )
            dataset.documentation.dataset_documentations.append(model.description)

        if model.columns is not None:
            for col in model.columns.values():
                column_name = col.name.lower()
                field = self._init_field(dataset, column_name)
                field.description = col.description
                field.native_type = (
                    "Not Set" if col.data_type is None else col.data_type
                )

                field_doc = self._init_field_doc(dataset, column_name)
                field_doc.documentation = col.description

        if model.depends_on is not None:
            dataset.upstream = DatasetUpstream()
            dataset.upstream.source_code_url = model.original_file_path

            if model.depends_on.nodes is not None:
                dataset.upstream.source_datasets = [
                    self._get_upstream(n, dataset_map, platform, self.account)
                    for n in model.depends_on.nodes
                ]

    def _parse_test(self, test: ParsedSchemaTestNode) -> None:
        if test.test_metadata is None:
            return

        test_kwargs = test.test_metadata.kwargs
        if test_kwargs is None:
            return

        model_arg = test_kwargs.get("model")
        if model_arg is None:
            return

        matches = re.findall("^{{ ref\\('([^'\" ]+)'\\) }}$", model_arg)
        if len(matches) == 0:
            return

        model_name = matches[0]

        assert test.database is not None
        table = DbtExtractor._dataset_name(test.database, test.schema_, model_name)
        dataset = self._metadata[table]

        combination_of_columns = test_kwargs.get("combination_of_columns")
        column_name = test_kwargs.get("column_name")

        if combination_of_columns is not None:
            columns = cast(List[str], test_kwargs.get("combination_of_columns"))
        elif column_name is not None:
            columns = [column_name]

        for column in columns:
            field_doc = self._init_field_doc(dataset, column)
            if not field_doc.tests:
                field_doc.tests = []
            field_doc.tests.append(test.test_metadata.name)

    def _parse_catalog(self, platform: str) -> None:
        if self._catalog is None:
            return

        nodes = self._catalog.nodes
        sources = self._catalog.sources

        for model in {**nodes, **sources}.values():
            self._parse_catalog_model(model, platform)

    def _parse_catalog_model(self, model: CatalogTable, platform: str):
        meta = model.metadata
        columns = model.columns
        stats = model.stats

        assert meta.database is not None
        dataset = self._init_dataset(meta.database, meta.schema_, meta.name, platform)

        # TODO (ch1236): Re-enable once we figure the source & expected format
        # self._init_ownership(dataset)
        # assert dataset.ownership is not None and dataset.ownership.people is not None
        # dataset.ownership.people.append(self._build_owner(meta["owner"]))

        self._init_schema(dataset)
        self._init_documentation(dataset)

        for col in columns.values():
            column_name = col.name.lower()
            field = self._init_field(dataset, column_name)
            field.description = col.comment
            field.native_type = "Not Set" if col.type is None else col.type

            field_doc = self._init_field_doc(dataset, column_name)
            field_doc.documentation = col.comment

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

    @staticmethod
    def _dataset_name(db: str, schema: str, table: str) -> str:
        return f"{db}.{schema}.{table}".lower()

    @staticmethod
    def _get_dataset_name(dataset: DbtDataset) -> str:
        return DbtExtractor._dataset_name(
            dataset.database, dataset.schema, dataset.name
        )

    @staticmethod
    def _build_entity(
        table_name: str, platform: str, account: Optional[str]
    ) -> Dataset:
        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            name=table_name, account=account, platform=DataPlatform[platform]
        )
        return dataset

    def _init_dataset(
        self, database: str, schema: str, name: str, platform: str
    ) -> Dataset:
        table = self._dataset_name(database, schema, name)
        if table not in self._metadata:
            self._metadata[table] = self._build_entity(table, platform, self.account)
        return self._metadata[table]

    @staticmethod
    def _init_schema(dataset: Dataset) -> None:
        if not dataset.schema:
            dataset.schema = DatasetSchema()
            dataset.schema.schema_type = SchemaType.SQL
            dataset.schema.fields = []

    @staticmethod
    def _init_field(dataset: Dataset, column: str) -> SchemaField:
        assert dataset.schema is not None and dataset.schema.fields is not None

        field = next((f for f in dataset.schema.fields if f.field_path == column), None)
        if not field:
            field = SchemaField()
            field.field_path = column
            dataset.schema.fields.append(field)
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

    @staticmethod
    def _get_upstream(
        node: str,
        dataset_map: Dict[str, DbtDataset],
        platform: str,
        account: Optional[str],
    ) -> str:
        dataset_id = DatasetLogicalID(
            platform=DataPlatform[platform],
            account=account,
            name=DbtExtractor._get_dataset_name(dataset_map[node]),
        )
        return str(EntityId(EntityType.DATASET, dataset_id))
