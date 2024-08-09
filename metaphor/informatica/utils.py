import json
from typing import Optional

from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.logger import get_logger
from metaphor.informatica.models import ConnectionDetail
from metaphor.models.metadata_change_event import DataPlatform, DatasetLogicalID

CONNECTOR_GUID_PLATFORM_MAP = {"com.infa.adapter.snowflake": DataPlatform.SNOWFLAKE}
CONNECTOR_PLATFORM_MAP = {"Oracle": DataPlatform.ORACLE}
SUPPORTED_CONNECTOR_TYPES = {"Oracle", "TOOLKIT_CCI"}

logger = get_logger()


def parse_error(content: str):
    try:
        return json.loads(content)
    except json.decoder.JSONDecodeError:
        return None


def get_platform(connection: ConnectionDetail) -> Optional[DataPlatform]:
    if connection.type not in SUPPORTED_CONNECTOR_TYPES:
        logger.warning(f"Unsupported connection type: {connection.type}")
        return None

    if connection.connectorGuid:
        platform = CONNECTOR_GUID_PLATFORM_MAP.get(connection.connectorGuid)
    else:
        platform = CONNECTOR_PLATFORM_MAP.get(connection.type)
    return platform


def get_account(connection: ConnectionDetail) -> Optional[DataPlatform]:
    if connection.host:
        return connection.host
    if connection.connParams:
        return connection.connParams.account
    return None


def init_dataset_logical_id(
    name: str,  # INFA format name
    connection: ConnectionDetail,
) -> Optional[DatasetLogicalID]:
    platform = get_platform(connection)
    if platform is None:
        logger.warning(
            f"Unsupported connector: {connection.connectorGuid}, {connection.type}"
        )
        return None

    parts = name.split("/")
    if len(parts) != 3:
        logger.warning(f"Invalid object name: {name}")
        return None

    [database, schema, table] = parts

    return DatasetLogicalID(
        name=dataset_normalized_name(db=database, schema=schema, table=table),
        platform=platform,
        account=get_account(connection),
    )
