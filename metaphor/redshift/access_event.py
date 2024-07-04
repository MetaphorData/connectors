from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator

from asyncpg import Connection, Record

REDSHIFT_USAGE_SQL_TEMPLATE = """
WITH queries AS (
    SELECT
        *,
        SUM(LEN(text)) OVER (PARTITION BY query) AS length
    FROM
        stl_querytext
), filtered AS (
    SELECT
        q.query,
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
        q.query
)
SELECT DISTINCT
    ss.userid,
    ss.query,
    sui.usename,
    ss.rows,
    ss.bytes,
    ss.tbl,
    q.querytxt,
    sti.database,
    sti.schema,
    sti.table,
    sq.starttime,
    sq.endtime,
    sq.aborted
FROM stl_scan ss
    JOIN svv_table_info sti ON ss.tbl = sti.table_id
    JOIN stl_query sq ON ss.query = sq.query
    JOIN filtered q ON ss.query = q.query
    JOIN svl_user_info sui ON sq.userid = sui.usesysid
WHERE ss.starttime >= '{start_time}'
    AND ss.starttime < '{end_time}'
    AND sq.aborted = 0
ORDER BY ss.endtime DESC;
"""
"""
The condition `length < 65536` is because Redshift's LISTAGG method
is unable to process the query if it is over 65535 characters long.
See https://docs.aws.amazon.com/redshift/latest/dg/r_WF_LISTAGG.html#r_WF_LISTAGG-data-types
"""


@dataclass(frozen=True)
class AccessEvent:
    userid: int
    query: int
    usename: str
    tbl: int
    rows: int
    bytes: int
    querytxt: str
    database: str
    schema: str
    table: str
    starttime: datetime
    endtime: datetime
    aborted: int

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

        record["starttime"] = record["starttime"].replace(tzinfo=timezone.utc)
        record["endtime"] = record["endtime"].replace(tzinfo=timezone.utc)

        record["querytxt"] = record["querytxt"].replace("\\n", "\n")

        return AccessEvent(**record)

    def table_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

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
