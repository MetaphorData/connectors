from datetime import datetime
from unittest.mock import MagicMock, patch

from metaphor.common.utils import to_utc_time
from metaphor.mssql.model import MssqlColumn, MssqlDatabase, MssqlForeignKey, MssqlTable
from metaphor.mssql.mssql_client import MssqlClient, mssql_fetch_all, pymssql

mssqlDatabase = MssqlDatabase(
    id=1,
    name="mock_database_name",
    create_time=to_utc_time(datetime.now()),
    collation_name="Latin1_General_100_CI_AS_SC_UTF8",
)

mssqlColumn1 = MssqlColumn(
    name="mock_column1",
    type="bigint",
    max_length=8.0,
    precision=19.0,
    is_nullable=False,
    is_unique=True,
    is_primary_key=True,
    is_foreign_key=False,
)

mssqlColumn2 = MssqlColumn(
    name="mock_column2",
    type="varchar",
    max_length=256.0,
    precision=0.0,
    is_nullable=True,
    is_primary_key=False,
)

mssqlClient = MssqlClient("mock-server.database.windows.net", "username", "password")


def test_get_database():
    rows = [
        [
            mssqlDatabase.id,
            mssqlDatabase.name,
            mssqlDatabase.create_time,
            mssqlDatabase.collation_name,
        ]
    ]
    with patch(
        "metaphor.mssql.mssql_client.mssql_fetch_all",
        return_value=rows,
    ):
        dbs = mssqlClient.get_databases()
        assert next(dbs) == mssqlDatabase


def test_get_tables():
    col_dict = {}
    col_dict[mssqlColumn1.name] = mssqlColumn1
    col_dict[mssqlColumn2.name] = mssqlColumn2
    table = MssqlTable(
        id="mock_table_id",
        name="mock_table",
        schema_name="dbo",
        type="U",
        create_time=to_utc_time(datetime.now()),
        column_dict=col_dict,
        is_external=False,
    )
    rows = []
    for column in table.column_dict.values():
        row = [
            table.id,
            table.name,
            table.schema_name,
            table.type,
            table.create_time,
            column.name,
            column.max_length,
            column.precision,
            column.is_nullable,
            column.type,
            column.is_unique,
            column.is_primary_key,
            table.is_external,
        ]
        rows.append(row)

    with patch(
        "metaphor.mssql.mssql_client.mssql_fetch_all",
        return_value=rows,
    ):
        tables = mssqlClient.get_tables("mock_database")
        assert len(tables) == 1
        assert tables[0] == table


def test_get_tables_with_external_resource():
    col_dict = {}
    col_dict[mssqlColumn1.name] = mssqlColumn1
    col_dict[mssqlColumn2.name] = mssqlColumn2
    table = MssqlTable(
        id="mock_table_id",
        name="mock_table",
        schema_name="dbo",
        type="U",
        create_time=to_utc_time(datetime.now()),
        column_dict=col_dict,
        is_external=True,
        external_source="http://mock_data_source",
        external_file_format="PARQUET",
    )
    rows = []
    for column in table.column_dict.values():
        row = [
            table.id,
            table.name,
            table.schema_name,
            table.type,
            table.create_time,
            column.name,
            column.max_length,
            column.precision,
            column.is_nullable,
            column.type,
            column.is_unique,
            column.is_primary_key,
            table.is_external,
        ]
        rows.append(row)

    external_data = [table.external_source, table.external_file_format]

    with patch(
        "metaphor.mssql.mssql_client.mssql_fetch_all",
        side_effect=[rows, [external_data]],
    ):
        tables = mssqlClient.get_tables("mock_database")
        assert len(tables) == 1
        assert tables[0] == table


def test_get_foreign_keys():
    foreign_key = MssqlForeignKey(
        name="mock_foreign_key_name",
        table_id="mock_table_id",
        column_name="mock_colume_name",
        referenced_table_id="mock_referenced_table_id",
        referenced_column="mock_referenced_column",
    )
    rows = [
        [
            foreign_key.name,
            foreign_key.table_id,
            foreign_key.column_name,
            foreign_key.referenced_table_id,
            foreign_key.referenced_column,
        ]
    ]

    with patch("metaphor.mssql.mssql_client.mssql_fetch_all", return_value=rows):
        foreign_keys = mssqlClient.get_foreign_keys("mock_database")
        assert next(foreign_keys) == foreign_key


def test_mssql_fetch_all():
    rows = [["mock_id_1", "mock_name_1"], ["mock_id_2", "mock_name_2"]]
    conn_instance = MagicMock()
    cursor_instance = MagicMock()
    conn_instance.cursor = MagicMock(return_value=cursor_instance)
    cursor_instance.execute = MagicMock()
    cursor_instance.fetchall = MagicMock(return_value=rows)
    cursor_instance.close = MagicMock()
    conn_instance.close = MagicMock()

    with patch.object(pymssql, "connect", return_value=conn_instance):
        res_rows = mssql_fetch_all(mssqlClient.config, "SELECT * FROM mock_table")

        assert len(res_rows) == len(rows)
        assert res_rows == rows
