import json
import logging
import re
import urllib.parse
from collections import defaultdict
from datetime import datetime
from itertools import chain
from typing import Collection, Dict, Generator, Iterator, List, Optional, Tuple

from databricks.sdk.service.catalog import TableInfo, TableType, VolumeInfo

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    to_dataset_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.models import to_dataset_statistics
from metaphor.common.query_history import user_id_or_email
from metaphor.common.utils import (
    md5_digest,
    safe_float,
    to_utc_datetime_from_timestamp,
    unique_list,
)
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
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
    SchemaType,
    SourceField,
    SourceInfo,
    SQLSchema,
    SystemContact,
    SystemContacts,
    SystemContactSource,
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
    FileInfo,
    NoPermission,
    TableLineage,
    extract_schema_field_from_column_info,
)
from metaphor.unity_catalog.utils import (
    build_query_log_filter_by,
    create_api,
    create_connection,
    list_column_lineage,
    list_table_lineage,
)

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

# For variable substitution in source URLs
URL_PATH_RE = re.compile(r"{path}")
URL_DATABASE_RE = re.compile(r"{catalog}")
URL_SCHEMA_RE = re.compile(r"{schema}")
URL_TABLE_RE = re.compile(r"{table}")


CatalogSystemTagsTuple = Tuple[List[SystemTag], Dict[str, List[SystemTag]]]
"""
(catalog system tags, schema name -> schema system tags)
"""


CatalogSystemTags = Dict[str, CatalogSystemTagsTuple]
"""
catalog name -> (catalog tags, schema name -> schema tags)
"""


def to_utc_from_timestamp_ms(timestamp_ms: Optional[int]):
    if timestamp_ms is not None:
        return to_utc_datetime_from_timestamp(timestamp_ms / 1000)
    return None


class UnityCatalogExtractor(BaseExtractor):
    """Unity Catalog metadata extractor"""

    _description = "Unity Catalog metadata crawler"
    _platform = Platform.UNITY_CATALOG

    @staticmethod
    def from_config_file(config_file: str) -> "UnityCatalogExtractor":
        return UnityCatalogExtractor(UnityCatalogRunConfig.from_yaml_file(config_file))

    def __init__(self, config: UnityCatalogRunConfig):
        super().__init__(config)
        self._hostname = config.hostname
        self._source_url = (
            f"https://{config.hostname}/{{path}}/{{catalog}}/{{schema}}/{{table}}"
            if config.source_url is None
            else config.source_url
        )

        self._api = create_api(f"https://{config.hostname}", config.token)
        self._connection = create_connection(
            token=config.token,
            server_hostname=config.hostname,
            http_path=config.http_path,
        )

        # Map fullname or volume path to a dataset
        self._datasets: Dict[str, Dataset] = {}
        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)
        self._query_log_config = config.query_log
        self._hierarchies: List[Hierarchy] = []
        self._volumes: Dict[str, VolumeInfo] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Unity Catalog")

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

                for volume in self._get_volume_infos(catalog, schema):
                    assert volume.full_name
                    self._volumes[volume.full_name] = volume
                    self._init_volume(volume)
                    self._extract_volume_files(volume)

                for table_info in self._get_table_infos(catalog, schema):
                    table_name = f"{catalog}.{schema}.{table_info.name}"
                    if table_info.name is None:
                        logger.error(f"Ignoring table without name: {table_info}")
                        continue
                    if not self._filter.include_table(catalog, schema, table_info.name):
                        logger.info(f"Ignore table: {table_name} due to filter config")
                        continue
                    dataset = self._init_dataset(table_info)
                    self._populate_lineage(dataset)

        self._fetch_tags(catalogs)

        entities: List[ENTITY_TYPES] = []
        entities.extend(list(self._datasets.values()))
        entities.extend(self._hierarchies)
        return entities

    def _get_catalogs(self) -> List[str]:
        catalogs = list(self._api.catalogs.list())
        json_dump_to_debug_file(catalogs, "list-catalogs.json")
        return [catalog.name for catalog in catalogs if catalog.name]

    def _get_schemas(self, catalog: str) -> List[str]:
        schemas = list(self._api.schemas.list(catalog))
        json_dump_to_debug_file(schemas, f"list-schemas-{catalog}.json")
        return [schema.name for schema in schemas if schema.name]

    def _get_table_infos(
        self, catalog: str, schema: str
    ) -> Generator[TableInfo, None, None]:
        tables = list(self._api.tables.list(catalog, schema))
        json_dump_to_debug_file(tables, f"list-tables-{catalog}-{schema}.json")
        for table in tables:
            yield table

    def _get_volume_infos(
        self, catalog: str, schema: str
    ) -> Generator[VolumeInfo, None, None]:
        volumes = list(self._api.volumes.list(catalog, schema))
        json_dump_to_debug_file(volumes, f"list-volumes-{catalog}-{schema}.json")
        for volume in volumes:
            yield volume

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
        assert table_info.catalog_name and table_info.schema_name and table_info.name
        table_name = table_info.name
        schema_name = table_info.schema_name
        database = table_info.catalog_name

        normalized_name = dataset_normalized_name(database, schema_name, table_name)

        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            name=normalized_name, platform=DataPlatform.UNITY_CATALOG
        )

        dataset.structure = DatasetStructure(
            database=database, schema=schema_name, table=table_name
        )

        if table_info.table_type is None:
            raise ValueError(f"Invalid table {table_info.name}, no table_type found")

        fields = []
        if table_info.columns is not None:
            fields = [
                extract_schema_field_from_column_info(column_info)
                for column_info in table_info.columns
            ]

        dataset.schema = DatasetSchema(
            schema_type=SchemaType.SQL,
            description=table_info.comment,
            fields=fields,
            sql_schema=SQLSchema(
                materialization=TABLE_TYPE_MATERIALIZATION_TYPE_MAP.get(
                    table_info.table_type, MaterializationType.TABLE
                ),
                table_schema=(
                    table_info.view_definition if table_info.view_definition else None
                ),
            ),
        )

        main_url = self._get_table_source_url(database, schema_name, table_name)
        dataset.source_info = SourceInfo(
            main_url=main_url,
            created_at_source=to_utc_from_timestamp_ms(
                timestamp_ms=table_info.created_at
            ),
            last_updated=to_utc_from_timestamp_ms(timestamp_ms=table_info.updated_at),
        )

        dataset.unity_catalog = UnityCatalog(
            dataset_type=UnityCatalogDatasetType.UNITY_CATALOG_TABLE,
            table_info=UnityCatalogTableInfo(
                type=UnityCatalogTableType[table_info.table_type.value],
                data_source_format=(
                    table_info.data_source_format.value
                    if table_info.data_source_format is not None
                    else None
                ),
                storage_location=table_info.storage_location,
                owner=table_info.owner,
                properties=(
                    [
                        KeyValuePair(key=k, value=json.dumps(v))
                        for k, v in table_info.properties.items()
                    ]
                    if table_info.properties is not None
                    else []
                ),
            ),
        )

        dataset.system_tags = SystemTags(tags=[])

        self._datasets[normalized_name] = dataset

        return dataset

    def _get_location_url(self, location_name: str):
        url = f"https://{self._hostname}/explore/location/{location_name}/browse"
        return url

    def _table_logical_id(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> DatasetLogicalID:
        name = dataset_normalized_name(catalog_name, schema_name, table_name)
        return DatasetLogicalID(name=name, platform=DataPlatform.UNITY_CATALOG)

    def _table_entity_id(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> str:
        logical_id = self._table_logical_id(catalog_name, schema_name, table_name)
        return str(to_dataset_entity_id_from_logical_id(logical_id))

    def _populate_lineage(self, dataset: Dataset):
        table_name = f"{dataset.structure.database}.{dataset.structure.schema}.{dataset.structure.table}"
        lineage = list_table_lineage(self._api.api_client, table_name)

        # Skip table without upstream
        if not lineage.upstreams:
            logging.info(f"Table {table_name} has no upstream")
            return

        source_datasets = self._process_table_lineage(dataset, lineage)
        field_mappings = self._process_column_lineage(dataset, source_datasets)
        unique_datasets = unique_list(source_datasets)
        dataset.entity_upstream = EntityUpstream(
            source_entities=unique_datasets, field_mappings=field_mappings
        )

    def _get_dataset_by_file_info(self, file_info: FileInfo) -> Optional[Dataset]:
        # if upstream is a external location file
        if file_info.securable_type == "EXTERNAL_LOCATION":
            return self._init_file(file_info)

        # if upstream is a volume file
        if file_info.securable_name and file_info.securable_type == "VOLUME":
            volume_dataset = self._datasets.get(file_info.securable_name)

            if volume_dataset:
                # Users could use underlying path, replace the path to volume path
                volume_prefix = (
                    f"/Volumes/{volume_dataset.logical_id.name.replace('.', '/')}"
                )
                assert volume_dataset.unity_catalog.volume_info
                volume_info = volume_dataset.unity_catalog.volume_info
                path = file_info.path.replace(
                    volume_info.storage_location, volume_prefix
                )
                return self._datasets.get(path)
        return None

    def _process_table_lineage(
        self, dataset: Dataset, lineage: TableLineage
    ) -> List[str]:
        source_datasets: List[str] = []
        for upstream in lineage.upstreams:
            table_info = upstream.tableInfo
            if table_info is not None and not isinstance(table_info, NoPermission):
                entity_id = self._table_entity_id(
                    table_info.catalog_name, table_info.schema_name, table_info.name
                )
                source_datasets.append(entity_id)
            file_info = upstream.fileInfo
            if file_info is not None and file_info.has_permission:
                upstream_file = self._get_dataset_by_file_info(file_info)

                if not upstream_file:
                    logger.warning(
                        f"cannot parse upstream volume file, {file_info.path}"
                    )
                    continue
                source_datasets.append(
                    str(to_dataset_entity_id_from_logical_id(upstream_file.logical_id))
                )

        for downstream in lineage.downstreams:
            file_info = downstream.fileInfo
            if file_info is not None and file_info.has_permission:
                downstream_file = self._get_dataset_by_file_info(file_info)
                if not downstream_file:
                    logger.warning(
                        f"cannot parse downstream volume file, {file_info.path}"
                    )
                    continue
                downstream_file.entity_upstream.source_entities = unique_list(
                    chain(
                        downstream_file.entity_upstream.source_entities,
                        [str(to_dataset_entity_id_from_logical_id(dataset.logical_id))],
                    )
                )
        return source_datasets

    def _process_column_lineage(self, dataset: Dataset, source_datasets: List[str]):
        table_name = f"{dataset.structure.database}.{dataset.structure.schema}.{dataset.structure.table}"

        # Add column-level lineage if available
        field_mappings = []
        has_permission_issues = False
        if dataset.schema is not None and dataset.schema.fields is not None:
            for field in dataset.schema.fields:
                column_name = field.field_name
                if column_name is not None:
                    column_lineage = list_column_lineage(
                        self._api.api_client, table_name, column_name
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
                        entity_id = str(
                            to_dataset_entity_id_from_logical_id(logical_id)
                        )
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

        return field_mappings

    def collect_query_logs(self) -> Iterator[QueryLog]:
        if self._query_log_config.lookback_days <= 0:
            return

        for query_info in self._api.query_history.list(
            filter_by=build_query_log_filter_by(self._query_log_config, self._api),
            include_metrics=True,
            max_results=self._query_log_config.max_results,
        ):
            if query_info.query_text is None or query_info.user_name is None:
                continue

            start_time = None
            if query_info.query_start_time_ms is not None:
                start_time = to_utc_from_timestamp_ms(
                    timestamp_ms=query_info.query_start_time_ms
                )

            user_id, email = user_id_or_email(query_info.user_name)

            query_log = QueryLog(
                id=f"{DataPlatform.UNITY_CATALOG.name}:{query_info.query_id}",
                duration=safe_float(query_info.duration),
                user_id=user_id,
                email=email,
                platform=DataPlatform.UNITY_CATALOG,
                query_id=query_info.query_id,
                sql=query_info.query_text,
                sql_hash=md5_digest(query_info.query_text.encode("utf-8")),
                start_time=start_time,
            )
            if query_info.metrics is not None:
                query_log.bytes_read = safe_float(query_info.metrics.read_remote_bytes)
                query_log.bytes_written = safe_float(
                    query_info.metrics.write_remote_bytes
                )
                query_log.rows_read = safe_float(query_info.metrics.rows_read_count)
                query_log.rows_written = safe_float(
                    query_info.metrics.rows_produced_count
                )

            yield query_log

    def _extract_hierarchies(self, catalog_system_tags: CatalogSystemTags) -> None:
        for catalog, (catalog_tags, schema_name_to_tag) in catalog_system_tags.items():
            if catalog_tags:
                self._hierarchies.append(
                    Hierarchy(
                        logical_id=HierarchyLogicalID(
                            path=[
                                DataPlatform.UNITY_CATALOG.value,
                                catalog.lower(),
                            ]
                        ),
                        system_tags=SystemTags(tags=catalog_tags),
                    )
                )
            for schema, schema_tags in schema_name_to_tag.items():
                if schema_tags:
                    self._hierarchies.append(
                        Hierarchy(
                            logical_id=HierarchyLogicalID(
                                path=[
                                    DataPlatform.UNITY_CATALOG.value,
                                    catalog.lower(),
                                    schema.lower(),
                                ]
                            ),
                            system_tags=SystemTags(tags=schema_tags),
                        )
                    )

    def _fetch_catalog_system_tags(self, catalog: str) -> CatalogSystemTagsTuple:
        with self._connection.cursor() as cursor:
            catalog_tags = []
            schema_tags: Dict[str, List[SystemTag]] = defaultdict(list)
            catalog_tags_query = f"SELECT tag_name, tag_value FROM {catalog}.information_schema.catalog_tags"
            cursor.execute(catalog_tags_query)
            for tag_name, tag_value in cursor.fetchall():
                tag = SystemTag(
                    key=tag_name,
                    value=tag_value,
                    system_tag_source=SystemTagSource.UNITY_CATALOG,
                )
                catalog_tags.append(tag)

            schema_tags_query = f"SELECT schema_name, tag_name, tag_value FROM {catalog}.information_schema.schema_tags"
            cursor.execute(schema_tags_query)
            for schema_name, tag_name, tag_value in cursor.fetchall():
                if self._filter.include_schema(catalog, schema_name):
                    tag = SystemTag(
                        key=tag_name,
                        value=tag_value,
                        system_tag_source=SystemTagSource.UNITY_CATALOG,
                    )
                    schema_tags[schema_name].append(tag)
        return catalog_tags, schema_tags

    def _assign_dataset_system_tags(
        self, catalog: str, catalog_system_tags: CatalogSystemTags
    ) -> None:
        for schema in self._api.schemas.list(catalog):
            if schema.name:
                for table in self._api.tables.list(catalog, schema.name):
                    normalized_dataset_name = dataset_normalized_name(
                        catalog, schema.name, table.name
                    )
                    dataset = self._datasets.get(normalized_dataset_name)
                    if dataset is not None:
                        assert dataset.system_tags
                        dataset.system_tags.tags = (
                            catalog_system_tags[catalog][0]
                            + catalog_system_tags[catalog][1][schema.name]
                        )

    def _extract_object_tags(
        self, catalog, columns: List[str], tag_schema_name: str
    ) -> None:
        with self._connection.cursor() as cursor:
            query = f"SELECT {', '.join(columns)} FROM {catalog}.information_schema.{tag_schema_name}"

            cursor.execute(query)
            for (
                catalog_name,
                schema_name,
                dataset_name,
                tag_name,
                tag_value,
            ) in cursor.fetchall():
                normalized_dataset_name = dataset_normalized_name(
                    catalog_name, schema_name, dataset_name
                )
                dataset = self._datasets.get(normalized_dataset_name)

                if dataset is None:
                    logger.warn(f"Cannot find {normalized_dataset_name} dataset")
                    continue

                assert dataset.system_tags and dataset.system_tags.tags is not None

                if tag_value:
                    tag = SystemTag(
                        key=tag_name,
                        system_tag_source=SystemTagSource.UNITY_CATALOG,
                        value=tag_value,
                    )
                else:
                    tag = SystemTag(
                        key=None,
                        system_tag_source=SystemTagSource.UNITY_CATALOG,
                        value=tag_name,
                    )
                dataset.system_tags.tags.append(tag)

    def _extract_table_tags(self, catalog: str) -> None:
        self._extract_object_tags(
            catalog,
            columns=[
                "catalog_name",
                "schema_name",
                "table_name",
                "tag_name",
                "tag_value",
            ],
            tag_schema_name="table_tags",
        )

    def _extract_volume_tags(self, catalog: str) -> None:
        self._extract_object_tags(
            catalog,
            columns=[
                "catalog_name",
                "schema_name",
                "volume_name",
                "tag_name",
                "tag_value",
            ],
            tag_schema_name="volume_tags",
        )

    def _extract_column_tags(self, catalog: str) -> None:
        with self._connection.cursor() as cursor:
            columns = [
                "catalog_name",
                "schema_name",
                "table_name",
                "column_name",
                "tag_name",
                "tag_value",
            ]
            query = f"SELECT {', '.join(columns)} FROM {catalog}.information_schema.column_tags"

            cursor.execute(query)
            for (
                catalog_name,
                schema_name,
                table_name,
                column_name,
                tag_name,
                tag_value,
            ) in cursor.fetchall():
                normalized_dataset_name = dataset_normalized_name(
                    catalog_name, schema_name, table_name
                )
                dataset = self._datasets.get(normalized_dataset_name)
                if dataset is None:
                    logger.warn(f"Cannot find {normalized_dataset_name} table")
                    continue

                tag = f"{tag_name}={tag_value}" if tag_value else tag_name

                assert (
                    dataset.schema is not None
                )  # Can't be None, we initialized it at `init_dataset`
                if dataset.schema.fields:
                    field = next(
                        (
                            f
                            for f in dataset.schema.fields
                            if f.field_name == column_name
                        ),
                        None,
                    )
                    if field is not None:
                        if not field.tags:
                            field.tags = []
                        field.tags.append(tag)

    def _fetch_tags(self, catalogs: List[str]):
        catalog_system_tags: CatalogSystemTags = {}

        for catalog in catalogs:
            if self._filter.include_database(catalog):
                catalog_system_tags[catalog] = self._fetch_catalog_system_tags(catalog)
                self._extract_hierarchies(catalog_system_tags)
                self._assign_dataset_system_tags(catalog, catalog_system_tags)
                self._extract_table_tags(catalog)
                self._extract_volume_tags(catalog)
                self._extract_column_tags(catalog)

    def _extract_volume_files(self, volume: VolumeInfo):
        volume_dataset = self._datasets.get(
            dataset_normalized_name(
                volume.catalog_name, volume.schema_name, volume.name
            )
        )

        if not volume_dataset:
            return

        with self._connection.cursor() as cursor:
            query = f"LIST '/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}'"

            cursor.execute(query)
            for path, name, size, modification_time in cursor.fetchall():
                last_updated = to_utc_from_timestamp_ms(timestamp_ms=modification_time)
                volume_file = self._init_volume_file(
                    path,
                    size,
                    last_updated,
                    entity_id=str(
                        to_dataset_entity_id_from_logical_id(volume_dataset.logical_id)
                    ),
                )

                if volume_dataset and volume_file:
                    assert (
                        volume_dataset.unity_catalog.volume_info.volume_files
                        is not None
                    )
                    volume_dataset.unity_catalog.volume_info.volume_files.append(
                        VolumeFile(
                            modification_time=last_updated,
                            name=name,
                            path=path,
                            size=float(size),
                            entity_id=str(
                                to_dataset_entity_id_from_logical_id(
                                    volume_file.logical_id
                                )
                            ),
                        )
                    )

    def _init_file(self, file_info: FileInfo) -> Optional[Dataset]:
        if file_info.securable_type != "EXTERNAL_LOCATION":
            return None

        path = file_info.path
        platform = (
            DataPlatform.S3
            if path.startswith("s3://")
            else (
                DataPlatform.AZURE_DATA_LAKE_STORAGE
                if path.startswith("abfs://") or path.startswith("abfss://")
                else None
            )
        )

        if not platform:
            return None

        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            name=path,
            platform=platform,
        )

        main_url = (
            self._get_location_url(file_info.securable_name)
            if file_info.securable_name
            else None
        )
        dataset.source_info = SourceInfo(
            main_url=main_url,
        )

        dataset.unity_catalog = UnityCatalog(
            dataset_type=UnityCatalogDatasetType.UNITY_CATALOG_EXTERNAL_LOCATION,
        )

        dataset.entity_upstream = EntityUpstream(source_entities=[])

        self._datasets[path] = dataset

        return dataset

    def _init_volume(self, volume: VolumeInfo):
        assert (
            volume.volume_type
            and volume.schema_name
            and volume.catalog_name
            and volume.name
        )

        schema_name = volume.schema_name
        catalog_name = volume.catalog_name
        name = volume.name
        full_name = dataset_normalized_name(catalog_name, schema_name, name)

        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            name=full_name,
            platform=DataPlatform.UNITY_CATALOG,
        )

        dataset.structure = DatasetStructure(
            database=catalog_name, schema=schema_name, table=volume.name
        )

        main_url = self._get_volume_source_url(catalog_name, schema_name, name)
        dataset.source_info = SourceInfo(
            main_url=main_url,
            last_updated=to_utc_from_timestamp_ms(timestamp_ms=volume.updated_at),
            created_at_source=to_utc_from_timestamp_ms(timestamp_ms=volume.created_at),
            created_by=volume.created_by,
            updated_by=volume.updated_by,
        )

        dataset.schema = DatasetSchema(
            description=volume.comment,
        )

        if volume.owner:
            dataset.system_contacts = SystemContacts(
                contacts=[
                    SystemContact(
                        email=volume.owner,
                        system_contact_source=SystemContactSource.UNITY_CATALOG,
                    )
                ]
            )

        dataset.unity_catalog = UnityCatalog(
            dataset_type=UnityCatalogDatasetType.UNITY_CATALOG_VOLUME,
            volume_info=UnityCatalogVolumeInfo(
                type=UnityCatalogVolumeType[volume.volume_type.value],
                volume_files=[],
                storage_location=volume.storage_location,
            ),
        )
        dataset.entity_upstream = EntityUpstream(source_entities=[])

        dataset.system_tags = SystemTags(tags=[])

        self._datasets[full_name] = dataset

        return dataset

    def _init_volume_file(
        self,
        path: str,
        size: int,
        last_updated: Optional[datetime],
        entity_id: str,
    ) -> Optional[Dataset]:
        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            # We use path as ID for file
            name=path,
            platform=DataPlatform.UNITY_CATALOG_VOLUME_FILE,
        )

        if last_updated:
            dataset.source_info = SourceInfo(last_updated=last_updated)

        dataset.unity_catalog = UnityCatalog(
            dataset_type=UnityCatalogDatasetType.UNITY_CATALOG_VOLUME_FILE,
            volume_entity_id=entity_id,
        )

        dataset.statistics = to_dataset_statistics(
            size_bytes=size,
        )

        dataset.entity_upstream = EntityUpstream(source_entities=[])

        self._datasets[path] = dataset

        return dataset
