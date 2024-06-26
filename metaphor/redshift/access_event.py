from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator

from asyncpg import Connection, Record

REDSHIFT_USAGE_SQL_TEMPLATE = """
SELECT DISTINCT
    ss.userid,
    ss.query,
    sui.usename,
    ss.rows,
    ss.bytes,
    ss.tbl,
    REPLACE(
        REPLACE(
            LISTAGG(
                CASE
                    WHEN LEN(RTRIM(sqt.text)) = 0 THEN sqt.text
                    ELSE RTRIM(sqt.text)
                END,
                ''
            )
            WITHIN GROUP (ORDER BY sqt.sequence)
            OVER (
                PARTITION BY sqt.userid, sqt.xid, sqt.pid, sqt.query
            ),
            '\r', ''
        ),
        '\\n', ''
    ) AS querytxt,
    sti.database,
    sti.schema,
    sti.table,
    sq.starttime,
    sq.endtime,
    sq.aborted
FROM stl_scan ss
    JOIN svv_table_info sti ON ss.tbl = sti.table_id
    JOIN stl_query sq ON ss.query = sq.query
    JOIN stl_querytext sqt ON ss.query = sqt.query
    JOIN svl_user_info sui ON sq.userid = sui.usesysid
WHERE ss.starttime >= '{start_time}'
    AND ss.starttime < '{end_time}'
    AND sq.aborted = 0
GROUP BY
    ss.userid,
    ss.query,
    sui.usename,
    ss.rows,
    ss.bytes,
    ss.tbl,
    sti.database,
    sti.schema,
    sti.table,
    sq.starttime,
    sq.endtime,
    sq.aborted,
    ss.endtime,
    sqt.text,
    sqt.userid,
    sqt.xid,
    sqt.pid,
    sqt.query,
    sqt.sequence
ORDER BY ss.endtime DESC;
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
