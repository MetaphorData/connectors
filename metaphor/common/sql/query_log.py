from dataclasses import dataclass, field
from typing import Optional

from metaphor.common.sql.process_query.config import ProcessQueryConfig
from metaphor.common.sql.process_query.process_query import process_query
from metaphor.common.utils import md5_digest
from metaphor.models.metadata_change_event import DataPlatform, QueryLog


@dataclass
class PartialQueryLog(QueryLog):
    id: Optional[str] = field(default=None, init=False)
    platform: Optional[DataPlatform] = field(default=None, init=False)
    query_id: Optional[str] = field(default=None, init=False)
    sql: Optional[str] = field(default=None, init=False)
    sql_hash: Optional[str] = field(default=None, init=False)


def process_and_init_query_log(
    query: str,
    platform: DataPlatform,
    process_query_config: ProcessQueryConfig,
    query_log: PartialQueryLog,
    query_id: Optional[str] = None,
    query_hash: Optional[str] = None,
) -> Optional[QueryLog]:
    sql_hash = query_hash or md5_digest(query.encode("utf-8"))
    query_id = query_id or sql_hash

    sql = process_query(
        query,
        platform,
        process_query_config,
        sql_hash,
    )

    if sql:
        return QueryLog(
            **query_log.__dict__,
            id=f"{platform.name}:{query_id}",
            sql=sql,
            query_id=query_id,
            sql_hash=sql_hash,
            platform=platform,
        )

    return None
