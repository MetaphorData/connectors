import json
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple
from unittest.mock import MagicMock, patch

from metaphor.common.base_config import OutputConfig
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.filter import DatasetFilter
from metaphor.models.metadata_change_event import (
    DataPlatform,
    DatasetLogicalID,
    HierarchyLogicalID,
    MaterializationType,
    QueriedDataset,
    SchemaField,
    SystemTag,
    SystemTagSource,
)
from metaphor.snowflake.config import SnowflakeQueryLogConfig, SnowflakeRunConfig
from metaphor.snowflake.extractor import SnowflakeExtractor
from metaphor.snowflake.utils import DatasetInfo, SnowflakeTableType


def make_snowflake_config(filter: Optional[DatasetFilter] = None):
    config = SnowflakeRunConfig(
        account="snowflake_account",
        user="user",
        password="password",
        output=OutputConfig(),
    )
    if filter:
        config.filter = filter

    return config


def test_table_url():
    account = "foo"
    full_name = "this.is.TesT"
    assert (
        SnowflakeExtractor.build_table_url(account, full_name)
        == "https://foo.snowflakecomputing.com/console#/data/tables/detail?databaseName=THIS&schemaName=IS&tableName=TEST"
    )


@patch("metaphor.snowflake.auth.connect")
def test_default_excludes(mock_connect: MagicMock):
    extractor = SnowflakeExtractor(
        make_snowflake_config(
            DatasetFilter(
                includes={"foo": None},
                excludes={"bar": None},
            )
        )
    )

    assert extractor._filter.includes == {"foo": None}
    assert extractor._filter.excludes == {
        "bar": None,
        "SNOWFLAKE": None,
        "*": {"INFORMATION_SCHEMA": None},
    }


database = "db"
schema = "schema"
table_name = "table1"
column = "col1"
table_type = SnowflakeTableType.BASE_TABLE.value
normalized_name = dataset_normalized_name(database, schema, table_name)


@patch("metaphor.snowflake.auth.connect")
def test_fetch_tables(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            (database, schema, table_name, table_type, "comment1", 10, 20000),
            (database, schema, "foo.bar", table_type, "", 0, 0),
        ]
    )

    extractor = SnowflakeExtractor(
        make_snowflake_config(
            DatasetFilter(
                includes={"db": None},
            )
        )
    )

    extractor._fetch_tables(mock_cursor, database)

    assert len(extractor._datasets) == 1
    dataset = extractor._datasets[normalized_name]
    assert dataset.logical_id == DatasetLogicalID(
        name=normalized_name,
        platform=DataPlatform.SNOWFLAKE,
        account="snowflake_account",
    )
    assert dataset.schema.sql_schema.materialization == MaterializationType.TABLE
    assert dataset.schema.description == "comment1"


@patch("metaphor.snowflake.auth.connect")
def test_fetch_columns(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            (schema, table_name, column, "int", 10, 1, "YES", 0, "comment1"),
            (schema, "foo.bar", column, "str", 0, 0, "NO", 0, ""),
        ]
    )

    extractor = SnowflakeExtractor(make_snowflake_config())

    dataset = extractor._init_dataset(
        database, schema, table_name, table_type, "", None, None
    )
    extractor._datasets[normalized_name] = dataset

    extractor._fetch_columns(mock_cursor, database)

    assert len(dataset.schema.fields) == 1
    assert dataset.schema.fields[0].field_path == column
    assert dataset.schema.fields[0].nullable
    assert dataset.schema.fields[0].description == "comment1"


@patch("metaphor.snowflake.auth.connect")
def test_fetch_table_info(mock_connect: MagicMock):
    table_info = DatasetInfo(
        database=database, schema=schema, name=table_name, type=table_type
    )

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.fetchone.return_value = {
        f"DDL_{normalized_name}": "ddl",
        f"UPDATED_{normalized_name}": 1,
    }

    extractor = SnowflakeExtractor(make_snowflake_config())

    extractor._conn = mock_conn

    dataset = extractor._init_dataset(
        database, schema, table_name, table_type, "", None, None
    )
    extractor._datasets[normalized_name] = dataset

    extractor._fetch_table_info({normalized_name: table_info}, False)

    assert dataset.schema.sql_schema.table_schema == "ddl"
    assert dataset.statistics.last_updated == datetime.utcfromtimestamp(0).replace(
        tzinfo=timezone.utc
    )


@patch("metaphor.snowflake.auth.connect")
def test_fetch_unique_keys(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            (None, database, schema, table_name, column, None, "unique_constraint"),
            (None, database, schema, "foo", column, None, ""),
            (None, database, schema, table_name, "non_exist_column", None, ""),
        ]
    )

    extractor = SnowflakeExtractor(make_snowflake_config())

    dataset = extractor._init_dataset(
        database, schema, table_name, table_type, "", None, None
    )
    dataset.schema.fields = [
        SchemaField(field_path=column, native_type="str", nullable=True, subfields=None)
    ]
    extractor._datasets[normalized_name] = dataset

    extractor._fetch_unique_keys(mock_cursor)

    assert dataset.schema.fields[0].is_unique


@patch("metaphor.snowflake.auth.connect")
def test_fetch_primary_keys(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            (None, database, schema, table_name, column, None, "primary_key"),
            (None, database, schema, "foo", column, None, ""),
        ]
    )

    extractor = SnowflakeExtractor(make_snowflake_config())

    dataset = extractor._init_dataset(
        database, schema, table_name, table_type, "", None, None
    )
    extractor._datasets[normalized_name] = dataset

    extractor._fetch_primary_keys(mock_cursor)

    assert dataset.schema.sql_schema.primary_key == [column]


@patch("metaphor.snowflake.auth.connect")
def test_fetch_tags(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            ("key1", "value1", "TABLE", database, schema, table_name, None),
            ("key1", "value1", "TABLE", database, schema, "foo", None),
            ("key1", "col_tag1", "COLUMN", database, schema, table_name, "col1"),
            ("foo", "bar", "COLUMN", database, schema, table_name, "bad_column"),
        ]
    )

    extractor = SnowflakeExtractor(make_snowflake_config())

    dataset = extractor._init_dataset(
        database, schema, table_name, table_type, "", None, None
    )
    dataset.schema.fields.append(SchemaField(field_path="col1", subfields=[]))
    extractor._datasets[normalized_name] = dataset

    extractor._fetch_tags(mock_cursor)

    assert dataset.schema.tags == ["key1=value1"]
    assert dataset.schema.fields[0].field_path == "col1"
    assert dataset.schema.fields[0].tags == ["key1=col_tag1"]


@patch("metaphor.snowflake.auth.connect")
def test_fetch_hierarchy_system_tags(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            ("foo", "bar", "DATABASE", None, None, table_name, None),
            ("baz", "qux", "SCHEMA", database, None, table_name, None),
            ("quux", "corge", "SCHEMA", database, None, table_name, None),
            ("grault", "garply", "DATABASE", None, None, table_name, None),
            ("bad", "bad", "DATABASE", None, None, None, None),
            ("not", "good", "SCHEMA", None, None, None, None),
            ("no", "db", "SCHEMA", None, None, table_name, None),
        ]
    )

    extractor = SnowflakeExtractor(make_snowflake_config())

    dataset = extractor._init_dataset(
        database, schema, table_name, table_type, "", None, None
    )
    extractor._datasets[normalized_name] = dataset

    extractor._fetch_tags(mock_cursor)

    assert dataset.schema.tags is None
    assert extractor._hierarchies.get(dataset_normalized_name(table_name)) is not None
    db_hierarchy = extractor._hierarchies[dataset_normalized_name(table_name)]
    assert db_hierarchy.logical_id == HierarchyLogicalID(
        path=[DataPlatform.SNOWFLAKE.value, table_name]
    )
    assert db_hierarchy.system_tags is not None
    assert db_hierarchy.system_tags == [
        SystemTag(key="foo", system_tag_source=SystemTagSource.SNOWFLAKE, value="bar"),
        SystemTag(
            key="grault", system_tag_source=SystemTagSource.SNOWFLAKE, value="garply"
        ),
    ]
    schema_hierarchy = extractor._hierarchies[
        dataset_normalized_name(database, table_name)
    ]
    assert schema_hierarchy.logical_id == HierarchyLogicalID(
        path=[DataPlatform.SNOWFLAKE.value, database, table_name]
    )
    assert schema_hierarchy.system_tags is not None
    assert schema_hierarchy.system_tags == [
        SystemTag(key="baz", system_tag_source=SystemTagSource.SNOWFLAKE, value="qux"),
        SystemTag(
            key="quux", system_tag_source=SystemTagSource.SNOWFLAKE, value="corge"
        ),
    ]


@patch("metaphor.snowflake.auth.connect")
def test_fetch_shared_databases(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter([(None, "INBOUND", None, database)])

    extractor = SnowflakeExtractor(make_snowflake_config())
    results = extractor._fetch_shared_databases(mock_cursor)

    assert results == [database]


@patch("metaphor.snowflake.auth.connect")
def test_parse_query_logs(mock_connect: MagicMock):
    query_logs: List[Tuple[Any, ...]] = [
        (
            "id1",  # QUERY_ID
            "METAPHOR",  # USER_NAME
            "short query text less than 40 chars",  # QUERY_TEXT
            "2022-12-12 14:01:02.778 -0800",  # START_TIME
            2514,  # TOTAL_ELAPSED_TIME
            "0.000296",  # CREDITS_USED_CLOUD_SERVICES
            "ACME",  # DATABASE_NAME
            "RIDE_SHARE",  # SCHEMA_NAME
            100,  # BYTES_SCANNED
            200,  # BYTES_WRITTEN
            10,  # ROWS_PRODUCED
            20,  # ROWS_INSERTED
            0,  # ROWS_UPDATED
            json.dumps(
                [
                    {
                        "columns": [
                            {"columnId": 1485364, "columnName": "START_STATION_NAME"},
                            {"columnId": 1485363, "columnName": "START_STATION_ID"},
                            {"columnId": 1485357, "columnName": "TOTAL_MINUTES"},
                            {
                                "columnId": 1485365,
                                "columnName": "START_STATION_BIKES_COUNT",
                            },
                            {"columnId": 1485360, "columnName": "MONTH"},
                            {
                                "columnId": 1485367,
                                "columnName": "START_STATION_INSTALL_DATE",
                            },
                            {"columnId": 1485361, "columnName": "START_PEAK_TRAVEL"},
                            {"columnId": 1485362, "columnName": "SAME_STATION_FLAG"},
                            {
                                "columnId": 1485366,
                                "columnName": "START_STATION_DOCKS_COUNT",
                            },
                            {"columnId": 1485358, "columnName": "TOTAL_BIKE_HIRES"},
                        ],
                        "objectDomain": "Table",
                        "objectId": 1471594,
                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                    }
                ]
            ),
            json.dumps(
                [
                    {
                        "columns": [
                            {
                                "baseSources": [
                                    {
                                        "columnName": "MONTH",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485909,
                                "columnName": "MONTH",
                                "directSources": [
                                    {
                                        "columnName": "MONTH",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "START_STATION_INSTALL_DATE",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485916,
                                "columnName": "START_STATION_INSTALL_DATE",
                                "directSources": [
                                    {
                                        "columnName": "START_STATION_INSTALL_DATE",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "TOTAL_BIKE_HIRES",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485907,
                                "columnName": "TOTAL_BIKE_HIRES",
                                "directSources": [
                                    {
                                        "columnName": "TOTAL_BIKE_HIRES",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "START_STATION_DOCKS_COUNT",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485915,
                                "columnName": "START_STATION_DOCKS_COUNT",
                                "directSources": [
                                    {
                                        "columnName": "START_STATION_DOCKS_COUNT",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "START_STATION_NAME",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485913,
                                "columnName": "START_STATION_NAME",
                                "directSources": [
                                    {
                                        "columnName": "START_STATION_NAME",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "START_STATION_ID",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485912,
                                "columnName": "START_STATION_ID",
                                "directSources": [
                                    {
                                        "columnName": "START_STATION_ID",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "TOTAL_BIKE_HIRES",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    },
                                    {
                                        "columnName": "TOTAL_MINUTES",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    },
                                ],
                                "columnId": 1485908,
                                "columnName": "AVERAGE_DURATION_IN_MINUTES",
                                "directSources": [
                                    {
                                        "columnName": "TOTAL_BIKE_HIRES",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    },
                                    {
                                        "columnName": "TOTAL_MINUTES",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    },
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "START_STATION_BIKES_COUNT",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485914,
                                "columnName": "START_STATION_BIKES_COUNT",
                                "directSources": [
                                    {
                                        "columnName": "START_STATION_BIKES_COUNT",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "SAME_STATION_FLAG",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485911,
                                "columnName": "SAME_STATION_FLAG",
                                "directSources": [
                                    {
                                        "columnName": "SAME_STATION_FLAG",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "START_PEAK_TRAVEL",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485910,
                                "columnName": "START_PEAK_TRAVEL",
                                "directSources": [
                                    {
                                        "columnName": "START_PEAK_TRAVEL",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "TOTAL_MINUTES",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485906,
                                "columnName": "TOTAL_HOURS",
                                "directSources": [
                                    {
                                        "columnName": "TOTAL_MINUTES",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                            {
                                "baseSources": [
                                    {
                                        "columnName": "TOTAL_MINUTES",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                                "columnId": 1485905,
                                "columnName": "TOTAL_MINUTES",
                                "directSources": [
                                    {
                                        "columnName": "TOTAL_MINUTES",
                                        "objectDomain": "Table",
                                        "objectId": 1471594,
                                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_2017",
                                    }
                                ],
                            },
                        ],
                        "objectDomain": "Table",
                        "objectId": 1472528,
                        "objectName": "ACME.RIDE_SHARE.RIDES_BY_MONTH_START_STATION_2017",
                    },
                    {
                        "objectDomain": "Table",
                        "objectId": 14725280,
                        "objectName": "ACME.RIDE_SHARE.FOO.BAR",
                    },
                ]
            ),
        ),
        (
            # Large query - expected to be ignored
            "id1",  # QUERY_ID
            "METAPHOR",  # USER_NAME
            "a very very long query that exceeds 40 chars",  # QUERY_TEXT
            "2022-12-12 14:01:02.778 -0800",  # START_TIME
            2514,  # TOTAL_ELAPSED_TIME
            "0.000296",  # CREDITS_USED_CLOUD_SERVICES
            "ACME",  # DATABASE_NAME
            "RIDE_SHARE",  # SCHEMA_NAME
            100,  # BYTES_SCANNED
            200,  # BYTES_WRITTEN
            10,  # ROWS_PRODUCED
            20,  # ROWS_INSERTED
            0,  # ROWS_UPDATED
        ),
    ]

    config = make_snowflake_config()
    config.query_log = SnowflakeQueryLogConfig(max_query_size=40)

    extractor = SnowflakeExtractor(config)
    extractor._parse_query_logs("1", query_logs)

    assert len(extractor._logs) == 1
    log0 = extractor._logs[0]
    assert log0.query_id == "id1"
    assert log0.bytes_read == 100
    assert log0.bytes_written == 200
    assert log0.rows_read == 10
    assert log0.rows_written == 20
    assert log0.sources == [
        QueriedDataset(
            id="DATASET~965CB9D50FF7E59D766536D8ED07E862",
            columns=[
                "START_STATION_NAME",
                "START_STATION_ID",
                "TOTAL_MINUTES",
                "START_STATION_BIKES_COUNT",
                "MONTH",
                "START_STATION_INSTALL_DATE",
                "START_PEAK_TRAVEL",
                "SAME_STATION_FLAG",
                "START_STATION_DOCKS_COUNT",
                "TOTAL_BIKE_HIRES",
            ],
            database="acme",
            schema="ride_share",
            table="rides_by_month_2017",
        )
    ]
    assert log0.targets == [
        QueriedDataset(
            id="DATASET~29B682EDD13A2758F47F073DD361FDBA",
            columns=[
                "MONTH",
                "START_STATION_INSTALL_DATE",
                "TOTAL_BIKE_HIRES",
                "START_STATION_DOCKS_COUNT",
                "START_STATION_NAME",
                "START_STATION_ID",
                "AVERAGE_DURATION_IN_MINUTES",
                "START_STATION_BIKES_COUNT",
                "SAME_STATION_FLAG",
                "START_PEAK_TRAVEL",
                "TOTAL_HOURS",
                "TOTAL_MINUTES",
            ],
            database="acme",
            schema="ride_share",
            table="rides_by_month_start_station_2017",
        )
    ]


@patch("metaphor.snowflake.auth.connect")
def test_fetch_schemas(mock_connect: MagicMock) -> None:
    mock_cursor = MagicMock()
    values = [("schema1"), ("schema2"), ("schema3")]
    mock_cursor.__iter__.return_value = iter(values)
    extractor = SnowflakeExtractor(make_snowflake_config())
    schemas = extractor._fetch_schemas(mock_cursor)
    assert schemas == [x[0] for x in values]


@patch("metaphor.snowflake.auth.connect")
def test_fetch_streams(mock_connect: MagicMock) -> None:
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            (
                "dont care",
                "STREAM",  # stream_name,
                "dont care",
                "dont care",
                "dont care",
                "some comment",  # comment
                "TABLE",  # source_name
                "Table",  # source_type_str
                "dont cate",
                "dont care",
                "true",  # stale
                "Default",  # stream_type_str
                "2023-01-01 12:00:00.000000+00:00",  # stale_after
            ),
            (
                "dont care",
                "BAD_STREAM_1",  # stream_name,
                "dont care",
                "dont care",
                "dont care",
                "some comment",  # comment
                "TABLE",  # source_name
                "Blah",  # bad source_type_str
                "dont cate",
                "dont care",
                "true",  # stale
                "Default",  # stream_type_str
                "2023-01-01 12:00:00.000000+00:00",  # stale_after
            ),
            (
                "dont care",
                "BAD_STREAM_2",  # stream_name,
                "dont care",
                "dont care",
                "dont care",
                "some comment",  # comment
                "TABLE",  # source_name
                "Table",  # source_type_str
                "dont cate",
                "dont care",
                "true",  # stale
                "bleh",  # bad stream_type_str
                "2023-01-01 12:00:00.000000+00:00",  # stale_after
            ),
        ]
    )
    mock_row_count_cursor = MagicMock()
    mock_row_count_cursor.fetchone = MagicMock()
    mock_row_count_cursor.fetchone.return_value = (3,)
    extractor = SnowflakeExtractor(make_snowflake_config())
    extractor._conn = MagicMock()
    extractor._conn.cursor = MagicMock()
    extractor._conn.cursor.return_value = mock_row_count_cursor
    extractor._fetch_streams(mock_cursor, "DB", "SCHEMA")
    normalized_name = dataset_normalized_name("DB", "SCHEMA", "STREAM")
    assert normalized_name in extractor._datasets
    assert len(extractor._datasets) == 1
    assert (
        extractor._datasets[normalized_name].schema.sql_schema.materialization
        is MaterializationType.STREAM
    )
