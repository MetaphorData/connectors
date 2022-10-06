from typing import Collection, Dict, Generator, List

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.unity_catalog.api import UnityCatalogApi

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatabaseFilter, DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    CustomMetadata,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    EntityType,
    MaterializationType,
    SchemaField,
    SchemaType,
    SQLSchema,
)
from metaphor.unity_catalog.config import UnityCatalogRunConfig
from metaphor.unity_catalog.models import Table, parse_table_from_object

logger = get_logger(__name__)

# Filter out "system" database & all "information_schema" schemas
DEFAULT_FILTER: DatabaseFilter = DatasetFilter(
    excludes={
        "system": None,
        "*": {"information_schema": None},
    }
)


class UnityCatalogExtractor(BaseExtractor):
    """Unity Catalog metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "UnityCatalogExtractor":
        return UnityCatalogExtractor(UnityCatalogRunConfig.from_yaml_file(config_file))

    def __init__(self, config: UnityCatalogRunConfig):
        super().__init__(
            config, "Unity Catalog metadata crawler", Platform.THOUGHT_SPOT
        )
        self._api = UnityCatalogExtractor.create_api(config.host, config.token)
        self._datasets: Dict[str, Dataset] = {}
        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)

    async def extract(self) -> Collection[ENTITY_TYPES]:
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
                    if not self._filter.include_table(catalog, schema, table.name):
                        logger.info(
                            f"Ignore table: {catalog}.{schema}.{table.name} due to filter config"
                        )
                        continue
                    self._init_dataset(table)
        return self._datasets.values()

    def _get_catalogs(self) -> List[str]:
        response = self._api.list_catalogs()
        catalogs = []
        for catalog in response.get("catalogs", []):
            if "name" in catalog:
                catalogs.append(catalog["name"])
        return catalogs

    def _get_schemas(self, catalog: str) -> List[str]:
        response = self._api.list_schemas(catalog_name=catalog, name_pattern=None)
        schemas = []
        for schema in response.get("schemas", []):
            if "name" in schema:
                schemas.append(schema["name"])
        return schemas

    def _get_tables(self, catalog: str, schema: str) -> Generator[Table, None, None]:
        response = self._api.list_tables(
            catalog_name=catalog, schema_name=schema, name_pattern=None
        )
        for table in response.get("tables", []):
            yield parse_table_from_object(table)

    def _init_dataset(self, table: Table):
        table_name = table.name
        schema_name = table.schema_name
        database = table.catalog_name

        full_name = f"{database}.{schema_name}.{table_name}".lower()

        dataset = Dataset()
        dataset.structure = DatasetStructure()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID(
            name=full_name, platform=DataPlatform.UNITY_CATALOG
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
                    nullable=column.nullable,
                )
                for column in table.columns
            ],
            sql_schema=SQLSchema(materialization=MaterializationType.TABLE),
        )

        dataset.custom_metadata = CustomMetadata(metadata=table.extra_metadata())

        self._datasets[full_name] = dataset

    @staticmethod
    def create_api(host: str, token: str) -> UnityCatalogApi:
        api_client = ApiClient(host=host, token=token)
        return UnityCatalogApi(api_client)
