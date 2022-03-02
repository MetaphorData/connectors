from typing import Optional

import attr
from metaphor.models.metadata_change_event import DataPlatform, DatasetLogicalID

from metaphor.common.entity_id import dataset_fullname


@attr.s(auto_attribs=True, str=True)
class MetaphorDataset:
    database: str
    schema: str
    table: str
    platform: DataPlatform
    snowflake_account: Optional[str] = None

    @staticmethod
    def to_dataset_logical_id(obj: "MetaphorDataset") -> DatasetLogicalID:
        account = None

        if obj.platform == DataPlatform.SNOWFLAKE:
            account = obj.snowflake_account

        return DatasetLogicalID(
            account, dataset_fullname(obj.database, obj.schema, obj.table), obj.platform
        )
