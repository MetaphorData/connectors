from typing import List, Optional

from pydantic import BaseModel, Field, TypeAdapter

from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import DataPlatform, QueriedDataset

logger = get_logger()


# The object types supported when parsing Access History view
# See https://docs.snowflake.com/en/sql-reference/account-usage/access_history#:~:text=(Named%20stage).-,objectDomain,-TEXT
SUPPORTED_OBJECT_DOMAIN_TYPES = (
    "TABLE",
    "VIEW",
    "MATERIALIZED VIEW",
    "STREAM",
)


class AccessedObjectColumn(BaseModel):
    columnId: Optional[int] = None
    columnName: str


class AccessedObject(BaseModel):
    """
    A Pydantic model for each element in the DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, and
    OBJECTS_MODIFIED columns of Snowflake's ACCESS_HISTORY view.

    See https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html
    """

    objectDomain: str = ""
    objectName: str = ""
    objectId: int = 0
    columns: List[AccessedObjectColumn] = Field(default_factory=lambda: list())


def parse_accessed_objects(raw_objects: str, account: str) -> List[QueriedDataset]:
    objects = TypeAdapter(List[AccessedObject]).validate_json(raw_objects)
    queried_datasets: List[QueriedDataset] = []
    for obj in objects:
        if (
            not obj.objectDomain
            or obj.objectDomain.upper() not in SUPPORTED_OBJECT_DOMAIN_TYPES
        ):
            continue

        table_name = obj.objectName.lower()
        parts = table_name.split(".")
        if len(parts) != 3:
            logger.debug(f"Invalid table name {table_name}, skip")
            continue

        dataset_id = str(
            to_dataset_entity_id(table_name, DataPlatform.SNOWFLAKE, account)
        )
        db, schema, table = parts

        queried_datasets.append(
            QueriedDataset(
                id=dataset_id,
                database=db,
                schema=schema,
                table=table,
                columns=[col.columnName for col in obj.columns] or None,
            )
        )

    return queried_datasets
