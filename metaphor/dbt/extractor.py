import json
import logging
import re
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from dataclasses_json import dataclass_json
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
    Ownership,
    PersonLogicalID,
    PersonOwner,
    SchemaField,
    SchemaType,
    SQLSchema,
)

from metaphor.common.entity_id import EntityId
from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor, RunConfig

from .dbt_model import DbtCatalog, DbtCatalogNode, DbtManifest, DbtManifestNode

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass_json
@dataclass
class DbtRunConfig(RunConfig):
    manifest: str
    catalog: str


class DbtExtractor(BaseExtractor):
    """DBT metadata extractor"""

    @staticmethod
    def config_class():
        return DbtRunConfig

    def __init__(self):
        self._manifest: Optional[DbtManifest] = None
        self._catalog: Optional[DbtCatalog] = None
        self._metadata: Dict[str, Dataset] = {}

    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, DbtExtractor.config_class())

        logger.info("Fetching metadata from DBT repo")

        try:
            self._manifest = DbtManifest.from_json_file(config.manifest)
        except Exception as e:
            logger.error(f"Read manifest json error: {e}")
            traceback.print_exc()
            return []

        try:
            self._catalog = DbtCatalog.from_json_file(config.catalog)
        except Exception as e:
            logger.error(f"Read catalog json error: {e}")
            traceback.print_exc()

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

        platform = metadata.adapter_type.upper()

        models = {k: v for (k, v) in nodes.items() if v.resource_type == "model"}
        tests = {k: v for (k, v) in nodes.items() if v.resource_type == "test"}
        datasets = {**models, **sources}

        for model in models.values():
            self._parse_model(model, platform, datasets)

        for test in tests.values():
            self._parse_test(test)

        return platform

    def _parse_model(
        self,
        model: DbtManifestNode,
        platform: str,
        datasets: Dict[str, DbtManifestNode],
    ) -> None:
        dataset = self._init_dataset(model.database, model.schema, model.name, platform)
        self._init_schema(dataset)

        if model.compiled_sql is not None:
            assert dataset.schema is not None
            dataset.schema.sql_schema = SQLSchema()
            dataset.schema.sql_schema.table_schema = model.compiled_sql

        if model.description is not None:
            self._init_documentation(dataset)
            assert (
                dataset.documentation is not None
                and dataset.documentation.dataset_documentations is not None
            )
            dataset.documentation.dataset_documentations.append(model.description)

        for col in model.columns.values():
            column_name = col.name.lower()
            field = self._init_field(dataset, column_name)
            field.description = col.description
            field.native_type = col.data_type

            field_doc = self._init_field_doc(dataset, column_name)
            field_doc.documentation = col.description

        if model.depends_on is not None:
            dataset.upstream = DatasetUpstream()
            dataset.upstream.source_code_url = model.original_file_path
            dataset.upstream.source_datasets = [
                self._get_upstream(n, platform, datasets)
                for n in model.depends_on.nodes
            ]

    def _parse_test(self, test: DbtManifestNode) -> None:
        assert test.test_metadata is not None

        test_meta = test.test_metadata
        model_arg = test_meta.kwargs.model
        matches = re.findall("^{{ ref\\('([^'\" ]+)'\\) }}$", model_arg)
        if len(matches) == 0:
            return

        model_name = matches[0]
        table = DbtExtractor._dataset_name(test.database, test.schema, model_name)
        column = test_meta.kwargs.column_name
        dataset = self._metadata[table]

        field_doc = self._init_field_doc(dataset, column)
        if not field_doc.tests:
            field_doc.tests = []
        field_doc.tests.append(test_meta.name)

    def _parse_catalog(self, platform: str) -> None:
        if self._catalog is None:
            return

        nodes = self._catalog.nodes
        sources = self._catalog.sources

        for model in {**nodes, **sources}.values():
            self._parse_catalog_model(model, platform)

    def _parse_catalog_model(self, model: DbtCatalogNode, platform: str):
        meta = model.metadata
        columns = model.columns
        stats = model.stats

        dataset = self._init_dataset(meta.database, meta.schema, meta.name, platform)

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
            field.native_type = col.type

            field_doc = self._init_field_doc(dataset, column_name)
            field_doc.documentation = col.comment

        if stats and stats.has_stats.value:
            dataset.statistics = DatasetStatistics()

            assert stats.row_count is not None
            dataset.statistics.record_count = float(stats.row_count.value)

            assert stats.bytes is not None
            dataset.statistics.data_size = (
                float(stats.bytes.value) / 1048576  # convert bytes to MB
            )

            assert stats.last_modified is not None
            # Must set tzinfo explicitly due to https://bugs.python.org/issue22377
            dataset.statistics.last_updated = datetime.strptime(
                stats.last_modified.value, "%Y-%m-%d %H:%M%Z"
            ).replace(tzinfo=timezone.utc)

    @staticmethod
    def _dataset_name(db: str, schema: str, table: str) -> str:
        return f"{db}.{schema}.{table}".lower()

    @staticmethod
    def _get_dataset_name(metadata: DbtManifestNode) -> str:
        return DbtExtractor._dataset_name(
            metadata.database, metadata.schema, metadata.name
        )

    @staticmethod
    def _build_entity(table_name: str, platform: str) -> Dataset:
        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID()
        dataset.logical_id.platform = DataPlatform[platform]
        dataset.logical_id.name = table_name
        return dataset

    def _init_dataset(
        self, database: str, schema: str, name: str, platform: str
    ) -> Dataset:
        table = self._dataset_name(database, schema, name)
        if table not in self._metadata:
            self._metadata[table] = self._build_entity(table, platform)
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
    def _init_ownership(dataset: Dataset) -> None:
        if not dataset.ownership:
            dataset.ownership = Ownership()
            dataset.ownership.people = []
            dataset.ownership.groups = []

    @staticmethod
    def _build_owner(db_owner: str) -> PersonOwner:
        owner = PersonOwner()
        owner.person = PersonLogicalID(db_owner)
        owner.role = "DB_TABLE_OWNER"
        return owner

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
        node: str, platform: str, datasets: Dict[str, DbtManifestNode]
    ) -> str:
        dataset_id = DatasetLogicalID()
        dataset_id.platform = DataPlatform[platform]
        dataset_id.name = DbtExtractor._get_dataset_name(datasets[node])
        return str(EntityId(EntityType.DATASET, dataset_id))
