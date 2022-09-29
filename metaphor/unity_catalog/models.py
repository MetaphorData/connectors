import json
from typing import List

from pydantic import BaseModel, parse_obj_as

from metaphor.common.logger import get_logger

logger = get_logger(__name__)


class Column(BaseModel):
    name: str
    type_name: str
    type_precision: int
    nullable: bool


class Table(BaseModel):
    name: str
    catalog_name: str
    schema_name: str
    table_type: str
    data_source_format: str
    columns: List[Column]
    storage_location: str
    owner: str
    updated_at: int
    updated_by: str


def parse_table_from_object(obj: object):
    logger.debug(f"table object: {json.dumps(obj)}")
    return parse_obj_as(Table, obj)
