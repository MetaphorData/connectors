import json
from typing import Optional

from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.logger import get_logger
from metaphor.informatica.models import ConnectionDetail
from metaphor.models.metadata_change_event import DataPlatform, DatasetLogicalID

CONNECTOR_PLATFORM_MAP = {"com.infa.adapter.snowflake": DataPlatform.SNOWFLAKE}

logger = get_logger()


def parse_error(content: str):
    try:
        return json.loads(content)
    except json.decoder.JSONDecodeError:
        return None


def init_dataset_logical_id(
    name: str,  # INFA format name
    connection: ConnectionDetail,
) -> Optional[DatasetLogicalID]:
    if connection.type != "TOOLKIT_CCI":
        logger.warning(f"Unsupported connection type: {connection.type}")
        return None

    account = connection.connParams.account if connection.connParams else None

    parts = name.split("/")
    if len(parts) != 3:
        logger.warning(f"Invalid object name: {name}")
        return None

    [database, schema, table] = parts

    platform = CONNECTOR_PLATFORM_MAP.get(connection.connectorGuid)

    if platform is None:
        logger.warning(f"Unsupported connector guid: {connection.connectorGuid}")
        return None

    return DatasetLogicalID(
        name=dataset_normalized_name(db=database, schema=schema, table=table),
        platform=platform,
        account=account,
    )
