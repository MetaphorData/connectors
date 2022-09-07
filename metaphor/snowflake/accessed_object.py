from typing import List, Optional

from pydantic import BaseModel, Field


class AccessedObjectColumn(BaseModel):
    columnId: Optional[int]
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
