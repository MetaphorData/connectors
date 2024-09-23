from dataclasses import dataclass
from typing import Any, List, Optional
from urllib.parse import ParseResult, parse_qs, urljoin, urlparse

import azure.mgmt.datafactory.models as DfModels

from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.logger import get_logger
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.common.utils import removesuffix
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    EntityUpstream,
    SchemaType,
)

logger = get_logger()


@dataclass
class LinkedService:
    database: Optional[str] = None
    account: Optional[str] = None
    project: Optional[str] = None
    url: Optional[str] = None


def get_abs_full_path(
    storage_account: str, abs_location: DfModels.AzureBlobStorageLocation
) -> str:
    parts: List[str] = [
        part  # type: ignore
        for part in [
            abs_location.container,
            abs_location.folder_path,
            abs_location.file_name,
        ]
        if part is not None and isinstance(part, str)
    ]

    full_path = urljoin(storage_account, "/".join(parts))

    return full_path


def get_adls_path(url: str, blob_fs_location: DfModels.AzureBlobFSLocation) -> str:
    parts: List[str] = [
        part  # type: ignore
        for part in [
            blob_fs_location.file_name,
            blob_fs_location.folder_path,
            blob_fs_location.file_system,
        ]
        if part is not None and isinstance(part, str)
    ]

    full_path = urljoin(url, "/".join(parts))

    return full_path


def init_dataset(
    name: Optional[str],
    platform: DataPlatform,
    schema_type: Optional[SchemaType] = None,
    account: Optional[str] = None,
) -> Dataset:
    return Dataset(
        logical_id=DatasetLogicalID(
            name=name,
            platform=platform,
            account=account,
        ),
        entity_upstream=EntityUpstream(source_entities=[]),
        schema=DatasetSchema(schema_type=schema_type) if schema_type else None,
    )


def init_snowflake_dataset(
    snowflake_dataset: DfModels.SnowflakeDataset, linked_service: LinkedService
) -> Dataset:
    schema = safe_get_from_json(snowflake_dataset.schema_type_properties_schema)
    table = safe_get_from_json(snowflake_dataset.table)
    database = linked_service.database

    return init_dataset(
        dataset_normalized_name(database, schema, table),
        DataPlatform.SNOWFLAKE,
        account=linked_service.account,
    )


def init_azure_sql_table_dataset(
    sql_table_dataset: DfModels.AzureSqlTableDataset, linked_service: LinkedService
) -> Dataset:
    schema = safe_get_from_json(sql_table_dataset.schema_type_properties_schema)
    table = safe_get_from_json(sql_table_dataset.table)

    return init_dataset(
        name=dataset_normalized_name(linked_service.database, schema, table),
        platform=DataPlatform.MSSQL,
        account=linked_service.account,
    )


def init_json_dataset(
    json_dataset: DfModels.JsonDataset, linked_service: LinkedService
) -> Optional[Dataset]:
    if (
        isinstance(json_dataset.location, DfModels.HttpServerLocation)
        and linked_service.url
    ):
        return init_dataset(
            name=linked_service.url,
            platform=DataPlatform.HTTP,
            schema_type=SchemaType.JSON,
        )

    if (
        isinstance(json_dataset.location, DfModels.AzureBlobFSLocation)
        and linked_service.url
    ):
        blob_fs_location = json_dataset.location

        adls_path = get_adls_path(linked_service.url, blob_fs_location)

        return init_dataset(
            name=adls_path,
            platform=DataPlatform.AZURE_DATA_LAKE_STORAGE,
            schema_type=SchemaType.JSON,
        )

    if (
        isinstance(json_dataset.location, DfModels.AzureBlobStorageLocation)
        and linked_service.account
    ):
        abs_location = json_dataset.location

        abs_full_path = get_abs_full_path(linked_service.account, abs_location)

        return init_dataset(
            name=abs_full_path,
            platform=DataPlatform.AZURE_BLOB_STORAGE,
            schema_type=SchemaType.JSON,
        )
    return None


def init_parquet_dataset(
    parquet_dataset: DfModels.ParquetDataset, linked_service: LinkedService
) -> Optional[Dataset]:
    if (
        isinstance(parquet_dataset.location, DfModels.AzureBlobFSLocation)
        and linked_service.url
    ):
        blob_fs_location = parquet_dataset.location

        adls_path = get_adls_path(linked_service.url, blob_fs_location)

        return init_dataset(
            name=adls_path,
            platform=DataPlatform.AZURE_DATA_LAKE_STORAGE,
            schema_type=SchemaType.PARQUET,
        )

    if (
        isinstance(parquet_dataset.location, DfModels.AzureBlobStorageLocation)
        and linked_service.account
    ):
        abs_full_path = get_abs_full_path(
            linked_service.account, parquet_dataset.location
        )

        return init_dataset(
            name=abs_full_path,
            platform=DataPlatform.AZURE_BLOB_STORAGE,
            schema_type=SchemaType.PARQUET,
        )
    return None


def init_rest_dataset(
    rest_dataset: DfModels.RestResourceDataset, linked_service: LinkedService
) -> Dataset:
    http_endpoint = linked_service.url

    return init_dataset(
        name=http_endpoint,
        platform=DataPlatform.HTTP,
    )


def init_delimited_text_dataset(
    delimited_text_dataset: DfModels.DelimitedTextDataset, linked_service: LinkedService
) -> Optional[Dataset]:
    if (
        isinstance(delimited_text_dataset.location, DfModels.AzureBlobFSLocation)
        and linked_service.url
    ):
        blob_fs_location = delimited_text_dataset.location

        adls_path = get_adls_path(linked_service.url, blob_fs_location)

        return init_dataset(
            name=adls_path,
            platform=DataPlatform.AZURE_DATA_LAKE_STORAGE,
            schema_type=SchemaType.SCHEMALESS,
        )

    if (
        isinstance(delimited_text_dataset.location, DfModels.AzureBlobStorageLocation)
        and linked_service.account
    ):
        abs_full_path = get_abs_full_path(
            linked_service.account, delimited_text_dataset.location
        )

        return init_dataset(
            name=abs_full_path,
            platform=DataPlatform.AZURE_BLOB_STORAGE,
            schema_type=SchemaType.SCHEMALESS,
        )
    return None


def process_snowflake_linked_service(
    snowflake_connection: DfModels.SnowflakeLinkedService, linked_service_name: str
) -> Optional[LinkedService]:
    connection_string = safe_get_from_json(snowflake_connection.connection_string)

    if connection_string is None:
        logger.warning(
            f"unknown connection string for {linked_service_name}, connection string: {connection_string}"
        )
        return None

    url: ParseResult = urlparse(connection_string)
    query_db = parse_qs(url.query or "").get("db")
    database = query_db[0] if query_db else None

    # extract snowflake account name from jdbc format, 'snowflake://<snowflake_host>/'
    hostname = urlparse(url.path).hostname
    snowflake_account = normalize_snowflake_account(hostname) if hostname else None

    return LinkedService(database=database, account=snowflake_account)


def process_azure_sql_linked_service(
    sql_database: DfModels.AzureSqlDatabaseLinkedService, linked_service_name: str
) -> Optional[LinkedService]:  # format: <key>=<value>;<key>=<value>
    connection_string = safe_get_from_json(sql_database.connection_string)

    if connection_string is None:
        logger.warning(
            f"unknown connection string for {linked_service_name}, connection string: {connection_string}"
        )
        return None

    server_host, database = None, None
    for kv_pair in connection_string.split(";"):
        [key, value] = kv_pair.split("=") if "=" in kv_pair else ["", ""]
        if key == "Data Source":
            server_host = (
                removesuffix(str(value), ".database.windows.net") if value else None
            )
        if key == "Initial Catalog":
            database = value

    return LinkedService(database=database, account=server_host)


def safe_get_from_json(prop: Any) -> Optional[str]:
    if isinstance(prop, str):
        return prop
    else:
        logger.warning(f"Unable to get value from {prop}")
        return None
