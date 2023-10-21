import json
import logging
import urllib.parse
from typing import Collection, Dict, Generator, List

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.unity_catalog.api import UnityCatalogApi

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    to_dataset_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.utils import safe_str, unique_list
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    DatasetUpstream,
    FieldMapping,
    KeyValuePair,
    MaterializationType,
    SchemaField,
    SchemaType,
    SourceField,
    SourceInfo,
    SQLSchema,
    UnityCatalog,
    UnityCatalogTableType,
)
from metaphor.unity_catalog.config import UnityCatalogRunConfig
from metaphor.unity_catalog.models import (
    NoPermission,
    Table,
    TableType,
    parse_table_from_object,
)
from metaphor.unity_catalog.utils import list_column_lineage, list_table_lineage

logger = get_logger()

# Filter out "system" database & all "information_schema" schemas
DEFAULT_FILTER: DatasetFilter = DatasetFilter(
    excludes={
        "system": None,
        "*": {"information_schema": None},
    }
)

TABLE_TYPE_MATERIALIZATION_TYPE_MAP = {
    TableType.MANAGED: MaterializationType.TABLE,
    TableType.EXTERNAL: MaterializationType.EXTERNAL,
    TableType.VIEW: MaterializationType.VIEW,
    TableType.MATERIALIZED_VIEW: MaterializationType.MATERIALIZED_VIEW,
}


class UnityCatalogExtractor(BaseExtractor):
    """Unity Catalog metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "UnityCatalogExtractor":
        return UnityCatalogExtractor(UnityCatalogRunConfig.from_yaml_file(config_file))

    def __init__(self, config: UnityCatalogRunConfig):
        super().__init__(
            config, "Unity Catalog metadata crawler", Platform.THOUGHT_SPOT
        )
        self._host = config.host
        self._token = config.token

        self._datasets: Dict[str, Dataset] = {}
        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Unity Catalog")

        self._api = UnityCatalogExtractor.create_api(self._host, self._token)

        catalogs = (
            self._get_catalogs()
            if self._filter.includes is None
            else list(self._filter.includes.keys())
        )
        for catalog in catalogs:
            schemas = self._get_schemas(catalog)
            for schema in schemas:
                if not self._filter.include_schema(catalog, schema):
                    logger.info(
                        f"Ignore schema: {catalog}.{schema} due to filter config"
                    )
                    continue

                for table in self._get_tables(catalog, schema):
                    table_name = f"{catalog}.{schema}.{table.name}"
                    if not self._filter.include_table(catalog, schema, table.name):
                        logger.info(f"Ignore table: {table_name} due to filter config")
                        continue
                    dataset = self._init_dataset(table)
                    self._populate_lineage(dataset)
        return self._datasets.values()

    def _get_catalogs(self) -> List[str]:
        response = self._api.list_catalogs()
        json_dump_to_debug_file(response, "list-catalogs.json")

        catalogs = []
        for catalog in response.get("catalogs", []):
            if "name" in catalog:
                catalogs.append(catalog["name"])
        return catalogs

    def _get_schemas(self, catalog: str) -> List[str]:
        response = self._api.list_schemas(catalog_name=catalog, name_pattern=None)
        json_dump_to_debug_file(response, f"list-schemas-{catalog}.json")

        schemas = []
        for schema in response.get("schemas", []):
            if "name" in schema:
                schemas.append(schema["name"])
        return schemas

    def _get_tables(self, catalog: str, schema: str) -> Generator[Table, None, None]:
        response = self._api.list_tables(
            catalog_name=catalog, schema_name=schema, name_pattern=None
        )
        json_dump_to_debug_file(response, f"list-tables-{catalog}-{schema}.json")
        for table in response.get("tables", []):
            yield parse_table_from_object(table)

    def _init_dataset(self, table: Table) -> Dataset:
        table_name = table.name
        schema_name = table.schema_name
        database = table.catalog_name

        normalized_name = dataset_normalized_name(database, schema_name, table_name)

        dataset = Dataset()
        dataset.structure = DatasetStructure()
        dataset.logical_id = DatasetLogicalID(
            name=normalized_name, platform=DataPlatform.UNITY_CATALOG
        )

        dataset.structure = DatasetStructure(
            database=database, schema=schema_name, table=table_name
        )

        dataset.schema = DatasetSchema(
            schema_type=SchemaType.SQL,
            description=table.comment,
            fields=[
                SchemaField(
                    subfields=None,
                    field_name=column.name,
                    field_path=column.name,
                    native_type=column.type_name,
                    precision=float(column.type_precision),
                )
                for column in table.columns
            ],
            sql_schema=SQLSchema(
                materialization=TABLE_TYPE_MATERIALIZATION_TYPE_MAP.get(
                    table.table_type, MaterializationType.TABLE
                ),
                table_schema=table.view_definition if table.view_definition else None,
            ),
        )

        path = urllib.parse.quote(
            f"/explore/data/{database}/{schema_name}/{table_name}"
        )
        dataset.source_info = SourceInfo(main_url=f"{self._host}{path}")

        dataset.unity_catalog = UnityCatalog(
            table_type=UnityCatalogTableType[table.table_type],
            data_source_format=safe_str(table.data_source_format),
            storage_location=table.storage_location,
            owner=table.owner,
            properties=[
                KeyValuePair(key=k, value=json.dumps(v))
                for k, v in table.properties.items()
            ],
        )

        self._datasets[normalized_name] = dataset

        return dataset

    def _table_logical_id(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> str:
        name = dataset_normalized_name(catalog_name, schema_name, table_name)
        return DatasetLogicalID(name=name, platform=DataPlatform.UNITY_CATALOG)

    def _table_entity_id(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> str:
        logical_id = self._table_logical_id(catalog_name, schema_name, table_name)
        return str(to_dataset_entity_id_from_logical_id(logical_id))

    def _populate_lineage(self, dataset: Dataset):
        table_name = f"{dataset.structure.database}.{dataset.structure.schema}.{dataset.structure.table}"
        lineage = list_table_lineage(self._api.client.client, table_name)

        # Skip table without upstream
        if not lineage.upstreams:
            logging.info(f"Table {table_name} has no upstream")
            return

        # Add table-level lineage
        source_datasets: List[str] = []
        for upstream in lineage.upstreams:
            table_info = upstream.tableInfo
            if table_info is None or isinstance(table_info, NoPermission):
                continue

            entity_id = self._table_entity_id(
                table_info.catalog_name, table_info.schema_name, table_info.name
            )
            source_datasets.append(entity_id)

        # Add column-level lineage if available
        field_mappings = []
        has_permission_issues = False
        for field in dataset.schema.fields:
            column_name = field.field_name
            column_lineage = list_column_lineage(
                self._api.client.client, table_name, column_name
            )

            field_mapping = FieldMapping(destination=column_name, sources=[])
            for upstream_col in column_lineage.upstream_cols:
                if isinstance(upstream_col, NoPermission):
                    has_permission_issues = True
                    continue

                logical_id = self._table_logical_id(
                    upstream_col.catalog_name,
                    upstream_col.schema_name,
                    upstream_col.table_name,
                )
                entity_id = str(to_dataset_entity_id_from_logical_id(logical_id))
                source_datasets.append(entity_id)
                field_mapping.sources.append(
                    SourceField(
                        dataset=logical_id,
                        source_entity_id=entity_id,
                        field=upstream_col.name,
                    )
                )
            field_mappings.append(field_mapping)

        if has_permission_issues:
            logger.error(
                f"Unable to extract lineage for {table_name} due to permission issues"
            )

        dataset.upstream = DatasetUpstream(
            source_datasets=unique_list(source_datasets), field_mappings=field_mappings
        )

    @staticmethod
    def create_api(host: str, token: str) -> UnityCatalogApi:
        api_client = ApiClient(host=host, token=token)
        return UnityCatalogApi(api_client)
