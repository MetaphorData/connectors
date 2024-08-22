from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator

from asyncpg import Connection, Record

REDSHIFT_USAGE_SQL_TEMPLATE = """
WITH queries AS (
    SELECT
        *,
        SUM(LEN(text)) OVER (PARTITION BY query_id) AS length
    FROM
        sys_query_text
), filtered AS (
    SELECT
        q.query_id,
        LISTAGG(
            CASE
                WHEN LEN(RTRIM(q.text)) = 0 THEN q.text
                ELSE RTRIM(q.text)
            END,
            ''
        ) WITHIN GROUP (ORDER BY q.sequence) AS querytxt
    FROM
        queries AS q
    WHERE
        q.length < 65536
    GROUP BY
        q.query_id
)
SELECT DISTINCT
    sqh.user_id,
    sqh.query_id,
    pu.usename,
    sqh.returned_rows AS rows,
    sqh.returned_bytes AS bytes,
    q.querytxt,
    trim(sqh.database_name) AS database,
    sqh.start_time,
    sqh.end_time
FROM sys_query_history sqh
    JOIN filtered q ON sqh.query_id = q.query_id
    JOIN pg_user_info pu ON sqh.user_id = pu.usesysid
WHERE sqh.status = 'success'
    AND sqh.start_time >= '{start_time}'
    AND sqh.start_time < '{end_time}'
ORDER BY sqh.end_time DESC;
"""
"""
The condition `length < 65536` is because Redshift's LISTAGG method
is unable to process the query if it is over 65535 characters long.
See https://docs.aws.amazon.com/redshift/latest/dg/r_WF_LISTAGG.html#r_WF_LISTAGG-data-types
"""


@dataclass(frozen=True)
class AccessEvent:
    user_id: int
    query_id: int
    usename: str
    rows: int
    bytes: int
    querytxt: str
    database: str
    end_time: datetime
    start_time: datetime

    @staticmethod
    def from_record(record: Record) -> "AccessEvent":
        # To convert mock Record to a dict
        if hasattr(record, "_asdict"):
            record = dict(record._asdict())
        else:
            record = dict(record)

        for k, v in record.items():
            if isinstance(v, str):
                record[k] = v.strip()

        record["start_time"] = record["start_time"].replace(tzinfo=timezone.utc)
        record["end_time"] = record["end_time"].replace(tzinfo=timezone.utc)

        record["querytxt"] = record["querytxt"].replace("\\n", "\n")

        return AccessEvent(**record)

    @staticmethod
    async def fetch_access_event(
        conn: Connection,
        start_date: datetime,
        end_date: datetime,
    ) -> AsyncIterator["AccessEvent"]:
        results = await conn.fetch(
            REDSHIFT_USAGE_SQL_TEMPLATE.format(
                start_time=start_date.isoformat(), end_time=end_date.isoformat()
            )
        )

        for record in results:
            yield AccessEvent.from_record(record)

        await conn.close()
