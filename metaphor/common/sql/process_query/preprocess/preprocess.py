from metaphor.common.sql.process_query.preprocess.snowflake import (
    drop_snowflake_copy_into_options,
)
from metaphor.models.metadata_change_event import DataPlatform


def preprocess(sql: str, data_platform: DataPlatform):
    if data_platform is DataPlatform.SNOWFLAKE:
        updated = drop_snowflake_copy_into_options(sql)
        return updated
    return sql
