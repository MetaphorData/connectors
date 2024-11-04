import json
import re
import urllib.parse
from typing import Collection, Dict, Iterator, List, Optional

from databricks.sdk.service.iam import ServicePrincipal
from pydantic import BaseModel

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    normalize_full_dataset_name,
    to_dataset_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import build_schema_field
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.common.utils import non_empty_str, safe_float
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AssetPlatform,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    EntityUpstream,
    FieldMapping,
    Hierarchy,
    HierarchyLogicalID,
    KeyValuePair,
    MaterializationType,
    QueryLog,
    SchemaField,
    SchemaType,
    SourceField,
    SourceInfo,
    SQLSchema,
    SystemContact,
    SystemContacts,
    SystemDescription,
    SystemTag,
    SystemTags,
    SystemTagSource,
    UnityCatalog,
    UnityCatalogDatasetType,
    UnityCatalogTableInfo,
    UnityCatalogTableType,
    UnityCatalogVolumeInfo,
    UnityCatalogVolumeType,
    VolumeFile,
)
from metaphor.unity_catalog.config import UnityCatalogRunConfig
from metaphor.unity_catalog.models import (
    CatalogInfo,
    ColumnInfo,
    SchemaInfo,
    TableInfo,
    Tag,
    VolumeFileInfo,
    VolumeInfo,
)
from metaphor.unity_catalog.queries import (
    ColumnLineageMap,
    TableLineageMap,
    list_catalogs,
    list_column_lineage,
    list_schemas,
    list_table_lineage,
    list_tables,
    list_volume_files,
    list_volumes,
)
from metaphor.unity_catalog.utils import (
    batch_get_last_refreshed_time,
    batch_get_table_properties,
    create_api,
    create_connection,
    create_connection_pool,
    get_query_logs,
    list_service_principals,
)

logger = get_logger()


TABLE_TYPE_MATERIALIZATION_TYPE_MAP = {
    "EXTERNAL": MaterializationType.EXTERNAL,
    "EXTERNAL_SHALLOW_CLONE": MaterializationType.EXTERNAL,
    "FOREIGN": MaterializationType.EXTERNAL,
    "MANAGED": MaterializationType.TABLE,
    "MANAGED_SHALLOW_CLONE": MaterializationType.TABLE,
    "MATERIALIZED_VIEW": MaterializationType.MATERIALIZED_VIEW,
    "STREAMING_TABLE": MaterializationType.STREAM,
    "VIEW": MaterializationType.VIEW,
}

TABLE_TYPE_WITH_HISTORY = set(
    [
        "EXTERNAL",
        "EXTERNAL_SHALLOW_CLONE",
        "MANAGED",
        "MANAGED_SHALLOW_CLONE",
    ]
)

TABLE_TYPE_MAP = {
    "EXTERNAL": UnityCatalogTableType.EXTERNAL,
    "EXTERNAL_SHALLOW_CLONE": UnityCatalogTableType.EXTERNAL_SHALLOW_CLONE,
    "FOREIGN": UnityCatalogTableType.FOREIGN,
    "MANAGED": UnityCatalogTableType.MANAGED,
    "MANAGED_SHALLOW_CLONE": UnityCatalogTableType.MANAGED_SHALLOW_CLONE,
    "MATERIALIZED_VIEW": UnityCatalogTableType.MATERIALIZED_VIEW,
    "STREAMING_TABLE": UnityCatalogTableType.STREAMING_TABLE,
    "VIEW": UnityCatalogTableType.VIEW,
}

# For variable substitution in source URLs
URL_PATH_RE = re.compile(r"{path}")
URL_DATABASE_RE = re.compile(r"{catalog}")
URL_SCHEMA_RE = re.compile(r"{schema}")
URL_TABLE_RE = re.compile(r"{table}")


class CatalogSystemTags(BaseModel):
    catalog_tags: List[SystemTag] = []
    schema_name_to_tags: Dict[str, List[SystemTag]] = {}


class UnityCatalogExtractor(BaseExtractor):
    """Unity Catalog metadata extractor"""

    _description = "Unity Catalog metadata crawler"
    _platform = Platform.UNITY_CATALOG

    @staticmethod
    def from_config_file(config_file: str) -> "UnityCatalogExtractor":
        return UnityCatalogExtractor(UnityCatalogRunConfig.from_yaml_file(config_file))

    def __init__(self, config: UnityCatalogRunConfig):
        super().__init__(config)
        self._token = config.token
        self._hostname = config.hostname
        self._http_path = config.http_path

        self._source_url = (
            f"https://{config.hostname}/{{path}}/{{catalog}}/{{schema}}/{{table}}"
            if config.source_url is None
            else config.source_url
        )

        self._describe_history_limit = config.describe_history_limit
        self._max_concurrency = config.max_concurrency

        self._has_select_permissions = config.has_select_permissions

        self._api = create_api(f"https://{config.hostname}", config.token)

        self._connection = create_connection(
            token=config.token,
            server_hostname=config.hostname,
            http_path=config.http_path,
        )

        self._service_principals: Dict[str, ServicePrincipal] = {}

        self._last_refresh_time_queue: List[str] = []
        self._table_properties_queue: List[str] = []

        # Map fullname or volume path to a dataset
        self._datasets: Dict[str, Dataset] = {}
        self._filter = config.filter.normalize()
        self._query_log_config = config.query_log
        self._hierarchies: Dict[str, Hierarchy] = {}
        self._volumes: Dict[str, VolumeInfo] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Unity Catalog")

        self._service_principals = list_service_principals(self._api)
        logger.info(f"Found service principals: {self._service_principals}")

        catalogs = list_catalogs(self._connection)
        for catalog_info in catalogs:
            catalog = catalog_info.catalog_name
            if not self._filter.include_database(catalog):
                logger.info(f"Ignore catalog {catalog} due to filter config")
                continue

            self._init_catalog(catalog_info)

            for schema_info in list_schemas(self._connection, catalog):
                schema = schema_info.schema_name
                if not self._filter.include_schema(catalog, schema):
                    logger.info(
                        f"Ignore schema {catalog}.{schema} due to filter config"
                    )
                    continue

                self._init_schema(schema_info)

                table_lineage = list_table_lineage(self._connection, catalog, schema)
                column_lineage = list_column_lineage(self._connection, catalog, schema)

                for volume_info in list_volumes(self._connection, catalog, schema):
                    self._volumes[volume_info.full_name] = volume_info
                    self._init_volume(volume_info)
                    self._extract_volume_files(volume_info)

                for table_info in list_tables(self._connection, catalog, schema):
                    table = table_info.table_name
                    table_name = f"{catalog}.{schema}.{table}"
                    if not self._filter.include_table(catalog, schema, table):
                        logger.info(f"Ignore table: {table_name} due to filter config")
                        continue

                    dataset = self._init_dataset(table_info)
                    self._populate_lineage(dataset, table_lineage, column_lineage)

        self._propagate_tags()

        # Batch query table properties and last refreshed time if granted SELECT permissions
        if self._has_select_permissions:
            self._populate_table_properties()
            self._populate_last_refreshed_time()

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._datasets.values())
        entities.extend(self._hierarchies.values())
        return entities

    def _get_table_source_url(
        self, database: str, schema_name: str, table_name: str
    ) -> str:
        return self._get_source_url("explore/data", database, schema_name, table_name)

    def _get_volume_source_url(
        self, database: str, schema_name: str, volume_name: str
    ) -> str:
        return self._get_source_url(
            "explore/data/volumes", database, schema_name, volume_name
        )

    def _get_source_url(
        self, path: str, database: str, schema_name: str, table_name: str
    ):
        url = self._source_url
        url = URL_PATH_RE.sub(urllib.parse.quote(path), url)
        url = URL_DATABASE_RE.sub(urllib.parse.quote(database), url)
        url = URL_SCHEMA_RE.sub(urllib.parse.quote(schema_name), url)
        url = URL_TABLE_RE.sub(urllib.parse.quote(table_name), url)
        return url

    def _init_dataset(self, table_info: TableInfo) -> Dataset:
        table_name = table_info.table_name
        schema_name = table_info.schema_name
        database = table_info.catalog_name
        table_type = table_info.type

        normalized_name = dataset_normalized_name(database, schema_name, table_name)

        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            name=normalized_name, platform=DataPlatform.UNITY_CATALOG
        )

        dataset.structure = DatasetStructure(
            database=database, schema=schema_name, table=table_name
        )

        fields = [self._init_column(column) for column in table_info.columns]

        dataset.schema = DatasetSchema(
            schema_type=SchemaType.SQL,
            description=non_empty_str(table_info.comment),
            fields=fields,
            sql_schema=SQLSchema(
                materialization=TABLE_TYPE_MATERIALIZATION_TYPE_MAP.get(
                    table_type,
                    MaterializationType.TABLE,
                ),
                table_schema=table_info.view_definition,
            ),
        )

        # Queue tables with history for batch query later
        if table_type in TABLE_TYPE_WITH_HISTORY:
            self._last_refresh_time_queue.append(normalized_name)

        main_url = self._get_table_source_url(database, schema_name, table_name)
        dataset.source_info = SourceInfo(
            main_url=main_url,
            created_at_source=table_info.created_at,
            created_by=table_info.created_by,
            last_updated=table_info.updated_at,
            updated_by=table_info.updated_by,
        )

        dataset.unity_catalog = UnityCatalog(
            dataset_type=UnityCatalogDatasetType.UNITY_CATALOG_TABLE,
            table_info=UnityCatalogTableInfo(
                type=TABLE_TYPE_MAP.get(
                    table_type,
                    UnityCatalogTableType.UNKNOWN,
                ),
                data_source_format=table_info.data_source_format,
                storage_location=table_info.storage_location,
                owner=table_info.owner,
            ),
        )

        # Queue non-view tables for batch query later
        if table_info.view_definition is None:
            self._table_properties_queue.append(normalized_name)

        dataset.system_contacts = SystemContacts(
            contacts=[
                SystemContact(
                    email=self._get_owner_display_name(table_info.owner),
                    system_contact_source=AssetPlatform.UNITY_CATALOG,
                )
            ]
        )

        dataset.system_tags = SystemTags(
            tags=[
                SystemTag(
                    key=tag.key,
                    value=tag.value,
                    system_tag_source=SystemTagSource.UNITY_CATALOG,
                )
                for tag in table_info.tags
            ]
        )

        self._datasets[normalized_name] = dataset

        return dataset

    def _init_column(self, column_info: ColumnInfo) -> SchemaField:
        field = build_schema_field(
            column_name=column_info.column_name,
            field_type=column_info.data_type,
            description=non_empty_str(column_info.comment),
            nullable=column_info.is_nullable,
            precision=safe_float(column_info.data_precision),
        )

        field.tags = [
            f"{tag.key}={tag.value}" if tag.key else tag.value
            for tag in column_info.tags
        ]

        return field

    def _get_location_url(self, location_name: str):
        url = f"https://{self._hostname}/explore/location/{location_name}/browse"
        return url

    def _table_logical_id(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> DatasetLogicalID:
        name = dataset_normalized_name(catalog_name, schema_name, table_name)
        return DatasetLogicalID(name=name, platform=DataPlatform.UNITY_CATALOG)

    def _table_entity_id(self, table_full_name: str) -> str:
        logical_id = DatasetLogicalID(
            name=normalize_full_dataset_name(table_full_name),
            platform=DataPlatform.UNITY_CATALOG,
        )

        return str(to_dataset_entity_id_from_logical_id(logical_id))

    def _populate_lineage(
        self,
        dataset: Dataset,
        table_lineage: TableLineageMap,
        column_lineage: ColumnLineageMap,
    ):
        source_datasets = self._process_table_lineage(dataset, table_lineage)
        field_mappings = self._process_column_lineage(dataset, column_lineage)

        if len(source_datasets) + len(field_mappings) > 0:
            dataset.entity_upstream = EntityUpstream(
                source_entities=source_datasets, field_mappings=field_mappings
            )

    def _get_full_table_name(self, dataset: Dataset):
        return f"{dataset.structure.database}.{dataset.structure.schema}.{dataset.structure.table}".lower()

    def _process_table_lineage(
        self, dataset: Dataset, lineage: TableLineageMap
    ) -> List[str]:
        source_table = self._get_full_table_name(dataset)
        if source_table not in lineage:
            return []

        source_datasets: List[str] = []
        for target_table in lineage[source_table].upstream_tables:
            entity_id = self._table_entity_id(target_table)
            source_datasets.append(entity_id)

        return source_datasets

    def _process_column_lineage(self, dataset: Dataset, lineage: ColumnLineageMap):
        source_table = self._get_full_table_name(dataset)
        if source_table not in lineage:
            return []

        field_mappings = []
        for target_column, source_columns in lineage[
            source_table
        ].upstream_columns.items():
            sources = []
            for source_column in source_columns:
                entity_id = self._table_entity_id(source_column.table_name)
                sources.append(
                    SourceField(
                        source_entity_id=entity_id,
                        field=source_column.column_name,
                    )
                )

            field_mappings.append(
                FieldMapping(destination=target_column, sources=sources)
            )

        return field_mappings

    def collect_query_logs(self) -> Iterator[QueryLog]:
        lookback_days = self._query_log_config.lookback_days
        if lookback_days <= 0:
            return

        logger.info(f"Fetching queries from the last {lookback_days} days")

        for query_log in get_query_logs(
            self._connection,
            self._query_log_config.lookback_days,
            self._query_log_config.excluded_usernames,
            self._service_principals,
            self._datasets,
            self._query_log_config.process_query,
        ):
            yield query_log

    def _init_hierarchy(
        self,
        catalog: str,
        schema: Optional[str] = None,
        owner: Optional[str] = None,
        comment: Optional[str] = None,
        tags: Optional[List[Tag]] = None,
    ) -> Hierarchy:
        path = [part.lower() for part in [catalog, schema] if part]
        hierarchy = self._hierarchies.setdefault(
            ".".join(path),
            Hierarchy(
                logical_id=HierarchyLogicalID(
                    path=[DataPlatform.UNITY_CATALOG.value] + path
                ),
            ),
        )

        if owner is not None:
            hierarchy.system_contacts = SystemContacts(
                contacts=[
                    SystemContact(
                        email=self._get_owner_display_name(owner),
                        system_contact_source=AssetPlatform.UNITY_CATALOG,
                    )
                ]
            )

        if comment:
            hierarchy.system_description = SystemDescription(
                description=comment,
                platform=AssetPlatform.UNITY_CATALOG,
            )

        if tags is not None:
            hierarchy.system_tags = SystemTags(
                tags=[
                    SystemTag(
                        key=tag.key,
                        value=tag.value,
                        system_tag_source=SystemTagSource.UNITY_CATALOG,
                    )
                    for tag in tags
                ]
            )

        return hierarchy

    def _init_catalog(
        self,
        catalog_info: CatalogInfo,
    ) -> Hierarchy:
        return self._init_hierarchy(
            catalog=catalog_info.catalog_name,
            owner=catalog_info.owner,
            comment=catalog_info.comment,
            tags=catalog_info.tags,
        )

    def _init_schema(
        self,
        schema_info: SchemaInfo,
    ) -> Hierarchy:
        return self._init_hierarchy(
            catalog=schema_info.catalog_name,
            schema=schema_info.schema_name,
            owner=schema_info.owner,
            comment=schema_info.comment,
            tags=schema_info.tags,
        )

    def _extract_volume_files(self, volume: VolumeInfo):
        catalog_name = volume.catalog_name
        schema_name = volume.schema_name
        volume_name = volume.volume_name

        volume_dataset = self._datasets.get(
            dataset_normalized_name(catalog_name, schema_name, volume_name)
        )

        if volume_dataset is None:
            return

        volume_entity_id = str(
            to_dataset_entity_id_from_logical_id(volume_dataset.logical_id)
        )

        for volume_file_info in list_volume_files(self._connection, volume):
            volume_file = self._init_volume_file(volume_file_info, volume_entity_id)
            assert volume_dataset.unity_catalog.volume_info.volume_files is not None

            volume_dataset.unity_catalog.volume_info.volume_files.append(
                VolumeFile(
                    modification_time=volume_file_info.last_updated,
                    name=volume_file_info.name,
                    path=volume_file_info.path,
                    size=volume_file_info.size,
                    entity_id=str(
                        to_dataset_entity_id_from_logical_id(volume_file.logical_id)
                    ),
                )
            )

    def _init_volume(self, volume: VolumeInfo):
        schema_name = volume.schema_name
        catalog_name = volume.catalog_name
        volume_name = volume.volume_name
        full_name = dataset_normalized_name(catalog_name, schema_name, volume_name)

        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            name=full_name,
            platform=DataPlatform.UNITY_CATALOG,
        )

        dataset.structure = DatasetStructure(
            database=catalog_name, schema=schema_name, table=volume_name
        )

        main_url = self._get_volume_source_url(catalog_name, schema_name, volume_name)
        dataset.source_info = SourceInfo(
            main_url=main_url,
            created_at_source=volume.created_at,
            created_by=volume.created_by,
            last_updated=volume.updated_at,
            updated_by=volume.updated_by,
        )

        dataset.schema = DatasetSchema(
            description=non_empty_str(volume.comment),
        )

        if volume.owner:
            dataset.system_contacts = SystemContacts(
                contacts=[
                    SystemContact(
                        email=volume.owner,
                        system_contact_source=AssetPlatform.UNITY_CATALOG,
                    )
                ]
            )

        dataset.unity_catalog = UnityCatalog(
            dataset_type=UnityCatalogDatasetType.UNITY_CATALOG_VOLUME,
            volume_info=UnityCatalogVolumeInfo(
                type=UnityCatalogVolumeType[volume.volume_type],
                volume_files=[],
                storage_location=volume.storage_location,
            ),
        )
        dataset.entity_upstream = EntityUpstream(source_entities=[])

        dataset.system_tags = SystemTags(
            tags=[
                SystemTag(
                    key=tag.key,
                    value=tag.value,
                    system_tag_source=SystemTagSource.UNITY_CATALOG,
                )
                for tag in volume.tags
            ]
        )

        self._datasets[full_name] = dataset

        return dataset

    def _init_volume_file(
        self,
        volume_file_info: VolumeFileInfo,
        volume_entity_id: str,
    ) -> Dataset:
        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            # We use path as ID for file
            name=volume_file_info.path,
            platform=DataPlatform.UNITY_CATALOG_VOLUME_FILE,
        )

        dataset.source_info = SourceInfo(last_updated=volume_file_info.last_updated)

        dataset.unity_catalog = UnityCatalog(
            dataset_type=UnityCatalogDatasetType.UNITY_CATALOG_VOLUME_FILE,
            volume_entity_id=volume_entity_id,
        )

        dataset.statistics = to_dataset_statistics(
            size_bytes=volume_file_info.size,
        )

        dataset.entity_upstream = EntityUpstream(source_entities=[])

        self._datasets[volume_file_info.path] = dataset

        return dataset

    def _propagate_tags(self):
        """Propagate tags from catalogs and schemas to tables & volumes"""

        for dataset in self._datasets.values():
            tags = []

            if dataset.structure is None:
                continue

            catalog_name = dataset.structure.database.lower()
            catalog = self._hierarchies.get(catalog_name)
            if catalog is not None and catalog.system_tags is not None:
                tags.extend(catalog.system_tags.tags)

            schema_name = dataset.structure.schema.lower()
            schema = self._hierarchies.get(f"{catalog_name}.{schema_name}")
            if schema is not None and schema.system_tags is not None:
                tags.extend(schema.system_tags.tags)

            if dataset.system_tags is not None:
                tags.extend(dataset.system_tags.tags)

            dataset.system_tags = SystemTags(tags=tags)

    def _populate_last_refreshed_time(self):
        connection_pool = create_connection_pool(
            self._token, self._hostname, self._http_path, self._max_concurrency
        )

        result_map = batch_get_last_refreshed_time(
            connection_pool,
            self._last_refresh_time_queue,
            self._describe_history_limit,
        )

        for name, last_refreshed_time in result_map.items():
            dataset = self._datasets.get(name)
            if dataset is not None:
                dataset.source_info = SourceInfo(
                    main_url=dataset.source_info.main_url,
                    created_at_source=dataset.source_info.created_at_source,
                    last_updated=last_refreshed_time,
                )

    def _populate_table_properties(self):
        connection_pool = create_connection_pool(
            self._token, self._hostname, self._http_path, self._max_concurrency
        )

        result_map = batch_get_table_properties(
            connection_pool,
            self._table_properties_queue,
        )

        for name, properties in result_map.items():
            dataset = self._datasets.get(name)
            if (
                dataset is None
                or dataset.unity_catalog is None
                or dataset.unity_catalog.table_info is None
            ):
                continue

            dataset.unity_catalog.table_info.properties = [
                KeyValuePair(key=k, value=json.dumps(v)) for k, v in properties.items()
            ]

    def _get_owner_display_name(self, user_id: str) -> str:
        # Unity Catalog returns service principal's application_id and must be
        # manually map back to display_name
        service_principal = self._service_principals.get(user_id)
        return (
            service_principal.display_name
            if service_principal and service_principal.display_name
            else user_id
        )
