from typing import Any, Dict, Iterator, List

from metaphor.common.logger import get_logger
from metaphor.common.utils import to_utc_time
from metaphor.mssql.model import (
    MssqlColumn,
    MssqlConnectConfig,
    MssqlDatabase,
    MssqlForeignKey,
    MssqlTable,
)

try:
    import pymssql
except ImportError:
    print("Please install metaphor[mssql] extra\n")
    raise

logger = get_logger()


def mssql_fetch_all(
    config: MssqlConnectConfig,
    query_str: str,
    database: str = "",
) -> List[Any]:
    rows = []
    try:
        server = config.endpoint
        database_str = f"{database}" if len(database) > 0 else ""
        conn = pymssql.connect(
            server=server,
            user=config.username,
            password=config.password,
            database=database_str,
            conn_properties="",
        )
        cursor = conn.cursor()
        cursor.execute(query_str)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception as ex:
        logger.exception(
            f"endpoint[{config.endpoint}] \n database:[{database}] \n sql query error: {ex}"
        )
    finally:
        return rows


class MssqlClient:
    def __init__(
        self,
        endpoint: str,
        username: str,
        password: str,
    ):
        self.config = MssqlConnectConfig(
            endpoint=endpoint, username=username, password=password
        )

    def get_databases(self) -> Iterator[MssqlDatabase]:
        query_str = """
            SELECT database_id, name, create_date, collation_name from sys.databases where name not in ('master', 'tempdb', 'model', 'msdb');
        """
        rows = mssql_fetch_all(self.config, query_str)
        for row in rows:
            yield MssqlDatabase(
                id=row[0],
                name=row[1],
                create_time=to_utc_time(row[2]),
                collation_name=row[3],
            )

    def get_tables(self, database_name: str) -> List[MssqlTable]:
        query_str = """
        SELECT t.object_id, t.name AS table_name, s.name AS schema_name
            ,t.type, t.create_date
            ,c.name AS column_name, c.max_length, c.precision
            ,c.is_nullable, typ.name as column_type
            ,idx.is_unique, idx.is_primary_key
            ,t.is_external
        FROM sys.tables AS t
        INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
        INNER JOIN sys.columns AS c ON t.object_id = c.object_id
        INNER JOIN sys.types As typ ON c.user_type_id = typ.user_type_id
        LEFT JOIN (
            SELECT ic.object_id, ic.column_id, i.is_unique, i.is_primary_key
            FROM sys.indexes AS i
            INNER JOIN sys.index_columns AS ic
                ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        ) AS idx ON t.object_id = idx.object_id AND c.column_id = idx.column_id
        """
        rows = mssql_fetch_all(self.config, query_str, database_name)

        table_dict: Dict[str, MssqlTable] = {}

        for row in rows:
            if row[0] not in table_dict:
                table = MssqlTable(
                    id=row[0],
                    name=row[1],
                    schema_name=row[2],
                    type=row[3],
                    create_time=to_utc_time(row[4]),
                    column_dict={},
                    is_external=row[12],
                )
                if table.is_external:
                    query_str = f"""
                        SELECT exd.location AS source, exf.format_type
                        FROM sys.external_tables AS ext
                        LEFT JOIN sys.external_data_sources AS exd
                        ON ext.data_source_id = exd.data_source_id
                        LEFT JOIN sys.external_file_formats AS exf
                        ON ext.file_format_id = exf.file_format_id
                        WHERE ext.object_id = '{row[0]}';
                    """
                    rows = mssql_fetch_all(self.config, query_str, database_name)
                    if len(rows) == 1:
                        table.external_source = rows[0][0]
                        table.external_file_format = rows[0][1]

                table_dict[row[0]] = table
            if row[5] in table_dict[row[0]].column_dict:
                if row[10]:
                    table_dict[row[0]].column_dict[row[5]].is_unique = True
                if row[11]:
                    table_dict[row[0]].column_dict[row[5]].is_primary_key = True
            else:
                table_dict[row[0]].column_dict[row[5]] = MssqlColumn(
                    name=row[5],
                    max_length=float(row[6]),
                    precision=float(row[7]),
                    is_nullable=row[8],
                    type=row[9],
                    is_unique=row[10],
                    is_primary_key=row[11],
                )

        return list(table_dict.values())

    def get_foreign_keys(self, database_name: str) -> Iterator[MssqlForeignKey]:
        query_str = """
        SELECT fk.name AS ForeignKeyName
            ,t_parent.object_id AS key_table_id
            ,c_parent.name AS key_column_name
            ,t_child.object_id AS referenced_table_id
            ,c_child.name AS referenced_column_name
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc
            ON fkc.constraint_object_id = fk.object_id
        INNER JOIN sys.tables t_parent
            ON t_parent.object_id = fk.parent_object_id
        INNER JOIN sys.columns c_parent
            ON fkc.parent_column_id = c_parent.column_id
            AND c_parent.object_id = t_parent.object_id
        INNER JOIN sys.tables t_child
            ON t_child.object_id = fk.referenced_object_id
        INNER JOIN sys.columns c_child
            ON c_child.object_id = t_child.object_id
            AND fkc.referenced_column_id = c_child.column_id
        ORDER BY t_parent.name, c_parent.name;
        """

        rows = mssql_fetch_all(self.config, query_str, database_name)

        for row in rows:
            yield MssqlForeignKey(
                name=row[0],
                table_id=row[1],
                column_name=row[2],
                referenced_table_id=row[3],
                referenced_column=row[4],
            )
