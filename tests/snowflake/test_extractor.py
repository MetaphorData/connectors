import json
from datetime import datetime, timezone
from typing import Optional
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
    SnowflakeStreamInfo,
    SnowflakeStreamSourceType,
    SnowflakeStreamType,
    SystemTag,
    SystemTags,
    SystemTagSource,
)
from metaphor.snowflake.config import SnowflakeConfig, SnowflakeQueryLogConfig
from metaphor.snowflake.extractor import SnowflakeExtractor
from metaphor.snowflake.utils import DatasetInfo, SnowflakeTableType


def make_snowflake_config(filter: Optional[DatasetFilter] = None):
    config = SnowflakeConfig(
        account="snowflake_account",
        user="user",
        password="password",
        output=OutputConfig(),
    )
    if filter:
        config.filter = filter

    return config


def test_table_url():
    account = "foo-bar"
    full_name = "this.is.TesT"
    assert (
        SnowflakeExtractor.build_table_url(account, full_name)
        == "https://app.snowflake.com/foo/bar/#/data/databases/THIS/schemas/IS/table/TEST"
    )

    assert (
        SnowflakeExtractor.build_table_url("legacy_account", "Its.Another.Test")
        == "https://legacy_account.snowflakecomputing.com/console#/data/tables/detail?databaseName=ITS&schemaName=ANOTHER&tableName=TEST"
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
            (
                database,
                schema,
                table_name,
                table_type,
                "comment1",
                10,
                20000,
                None,
            ),
            (
                database,
                schema,
                "foo.bar",
                table_type,
                "",
                0,
                0,
                datetime.fromisoformat("2024-01-01"),
            ),
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
def test_fetch_secure(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            (
                database,
                schema,
                table_name,
                True,
            ),
            (
                database,
                schema,
                "foo.bar",
                False,
            ),
        ]
    )

    extractor = SnowflakeExtractor(
        make_snowflake_config(
            DatasetFilter(
                includes={"db": None},
            )
        )
    )

    secure_views = extractor._fetch_secure_views(mock_cursor)

    assert len(secure_views) == 1
    assert f"{database}.{schema}.{table_name}" in secure_views
    assert f"{database}.{schema}.foo.bar" not in secure_views


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

    extractor._fetch_table_info({normalized_name: table_info}, False, set())

    assert dataset.schema.sql_schema.table_schema == "ddl"
    assert dataset.statistics.last_updated == datetime.utcfromtimestamp(0).replace(
        tzinfo=timezone.utc
    )


@patch("metaphor.snowflake.auth.connect")
def test_fetch_table_info_error_handling(mock_connect: MagicMock):
    table_info = DatasetInfo(
        database=database, schema=schema, name=table_name, type=table_type
    )

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.cursor.return_value = mock_cursor

    def should_raise(_0, _1):
        raise ValueError()

    mock_cursor.execute.side_effect = should_raise

    extractor = SnowflakeExtractor(make_snowflake_config())

    extractor._conn = mock_conn

    dataset = extractor._init_dataset(
        database, schema, table_name, table_type, "", None, None
    )
    extractor._datasets[normalized_name] = dataset

    extractor._fetch_table_info({normalized_name: table_info}, False, set())

    assert dataset.schema.sql_schema.table_schema is None
    assert dataset.statistics.last_updated is None


@patch("metaphor.snowflake.auth.connect")
def test_fetch_table_info_with_unknown_type(mock_connect: MagicMock):
    extractor = SnowflakeExtractor(make_snowflake_config())

    extractor._conn = mock_connect
    dataset = extractor._init_dataset(
        "db",
        "schema",
        "table",
        "BAD_TYPE",
        "comment",
        None,
        None,
        None,
    )
    assert dataset.schema.sql_schema.materialization is None


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

    assert dataset.system_tags.tags == [
        SystemTag(
            key="key1", system_tag_source=SystemTagSource.SNOWFLAKE, value="value1"
        )
    ]
    assert dataset.schema.fields[0].field_path == "col1"
    assert dataset.schema.fields[0].tags == ["key1=col_tag1"]


@patch("metaphor.snowflake.auth.connect")
def test_fetch_tags_for_similar_schema(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            ("foo", "foo", "SCHEMA", "db", None, "foo", None),
            ("foobar", "foo", "SCHEMA", "db", None, "foobar", None),
            ("foobaz", "foo", "SCHEMA", "db", None, "foobaz", None),
        ]
    )

    extractor = SnowflakeExtractor(make_snowflake_config())

    for i, schema in enumerate(["foo", "foobar", "foobaz"]):
        extractor._datasets[dataset_normalized_name("db", schema, f"table{i}")] = (
            extractor._init_dataset(
                "db", schema, f"table{i}", table_type, "", None, None
            )
        )

    extractor._fetch_tags(mock_cursor)

    assert "db.foo.table0" in extractor._datasets
    assert extractor._datasets["db.foo.table0"].system_tags.tags == [
        SystemTag(key="foo", system_tag_source=SystemTagSource.SNOWFLAKE, value="foo")
    ]

    assert "db.foobar.table1" in extractor._datasets
    assert extractor._datasets["db.foobar.table1"].system_tags.tags == [
        SystemTag(
            key="foobar", system_tag_source=SystemTagSource.SNOWFLAKE, value="foo"
        )
    ]

    assert "db.foobaz.table2" in extractor._datasets
    assert extractor._datasets["db.foobaz.table2"].system_tags.tags == [
        SystemTag(
            key="foobaz", system_tag_source=SystemTagSource.SNOWFLAKE, value="foo"
        )
    ]


@patch("metaphor.snowflake.auth.connect")
def test_fetch_hierarchy_system_tags(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            ("foo", "bar", "DATABASE", None, None, table_name, None),
            ("grault", "garply", "DATABASE", None, None, table_name, None),
            ("bad", "bad", "DATABASE", None, None, None, None),
            ("bad", "db", "DATABASE", None, None, "bad_db", None),
            ("baz", "qux", "SCHEMA", database, None, table_name, None),
            ("quux", "corge", "SCHEMA", database, None, table_name, None),
            ("not", "good", "SCHEMA", None, None, None, None),
            ("no", "db", "SCHEMA", None, None, table_name, None),
            ("bad", "schema", "SCHEMA", database, None, "bad_schema", None),
        ]
    )

    extractor = SnowflakeExtractor(make_snowflake_config())
    extractor._filter = DatasetFilter(
        excludes={
            database: {
                "bad_schema": None,
            },
            "bad_db": None,
        }
    )

    dataset = extractor._init_dataset(
        database, schema, table_name, table_type, "", None, None
    )
    extractor._datasets[normalized_name] = dataset

    extractor._fetch_tags(mock_cursor)

    assert dataset.system_tags and dataset.system_tags.tags is not None
    assert extractor._hierarchies.get(dataset_normalized_name(table_name)) is not None
    db_hierarchy = extractor._hierarchies[dataset_normalized_name(table_name)]
    assert db_hierarchy.logical_id == HierarchyLogicalID(
        path=[DataPlatform.SNOWFLAKE.value, table_name]
    )
    assert db_hierarchy.system_tags is not None
    assert db_hierarchy.system_tags == SystemTags(
        tags=[
            SystemTag(
                key="foo", system_tag_source=SystemTagSource.SNOWFLAKE, value="bar"
            ),
            SystemTag(
                key="grault",
                system_tag_source=SystemTagSource.SNOWFLAKE,
                value="garply",
            ),
        ]
    )
    schema_hierarchy = extractor._hierarchies[
        dataset_normalized_name(database, table_name)
    ]
    assert schema_hierarchy.logical_id == HierarchyLogicalID(
        path=[DataPlatform.SNOWFLAKE.value, database, table_name]
    )
    assert schema_hierarchy.system_tags is not None
    assert schema_hierarchy.system_tags == SystemTags(
        tags=[
            SystemTag(
                key="baz", system_tag_source=SystemTagSource.SNOWFLAKE, value="qux"
            ),
            SystemTag(
                key="quux", system_tag_source=SystemTagSource.SNOWFLAKE, value="corge"
            ),
        ]
    )


@patch("metaphor.snowflake.auth.connect")
def test_fetch_shared_databases(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.side_effect = [
        iter([("shared_1",), ("shared_3",)]),
    ]

    extractor = SnowflakeExtractor(make_snowflake_config())
    results = extractor._fetch_shared_databases(mock_cursor)

    assert results == ["shared_1", "shared_3"]


@patch("metaphor.snowflake.extractor.check_access_history")
@patch("metaphor.snowflake.extractor.fetch_query_history_count")
@patch("metaphor.snowflake.auth.connect")
def test_collect_query_logs(
    mock_connect: MagicMock,
    mock_fetch_query_history_count: MagicMock,
    mock_check_access_history: MagicMock,
):
    mock_check_access_history.return_value = True
    mock_fetch_query_history_count.return_value = 1

    class MockCursor:
        def execute(self, _query, _params):
            pass

        def __iter__(self):
            for obj in [
                (
                    "id1",  # QUERY_ID
                    "hash1",  # QUERY_PARAMETERIZED_HASH
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
                                    {
                                        "columnId": 1485364,
                                        "columnName": "START_STATION_NAME",
                                    },
                                    {
                                        "columnId": 1485363,
                                        "columnName": "START_STATION_ID",
                                    },
                                    {
                                        "columnId": 1485357,
                                        "columnName": "TOTAL_MINUTES",
                                    },
                                    {
                                        "columnId": 1485365,
                                        "columnName": "START_STATION_BIKES_COUNT",
                                    },
                                    {"columnId": 1485360, "columnName": "MONTH"},
                                    {
                                        "columnId": 1485367,
                                        "columnName": "START_STATION_INSTALL_DATE",
                                    },
                                    {
                                        "columnId": 1485361,
                                        "columnName": "START_PEAK_TRAVEL",
                                    },
                                    {
                                        "columnId": 1485362,
                                        "columnName": "SAME_STATION_FLAG",
                                    },
                                    {
                                        "columnId": 1485366,
                                        "columnName": "START_STATION_DOCKS_COUNT",
                                    },
                                    {
                                        "columnId": 1485358,
                                        "columnName": "TOTAL_BIKE_HIRES",
                                    },
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
                    "id2",  # QUERY_ID
                    "hash2",  # QUERY_PARAMETERIZED_HASH
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
            ]:
                yield obj

    config = make_snowflake_config()
    config.query_log = SnowflakeQueryLogConfig(max_query_size=40)

    extractor = SnowflakeExtractor(config)
    conn_instance = MagicMock()
    conn_instance.cursor.return_value = MockCursor()
    mock_connect.return_value = conn_instance
    query_logs = list(extractor.collect_query_logs())

    assert len(query_logs) == 1
    log0 = query_logs[0]
    assert log0.query_id == "id1"
    assert log0.bytes_read == 100
    assert log0.bytes_written == 200
    assert log0.rows_read == 10
    assert log0.rows_written == 20
    assert log0.sql == "short query text less than 40 chars"
    assert log0.sql_hash == "hash1"
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
                "DEFAULT",  # stream_type_str
                datetime.fromisoformat("2023-01-01 12:00:00+00:00"),  # stale_after
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
                "DEFAULT",  # stream_type_str
                datetime.fromisoformat("2023-01-01 12:00:00+00:00"),  # stale_after
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
                datetime.fromisoformat("2023-01-01 12:00:00+00:00"),  # stale_after
            ),
        ]
    )
    mock_row_count_cursor = MagicMock()
    mock_row_count_cursor.fetchone = MagicMock()
    mock_row_count_cursor.fetchone.return_value = (3,)

    extractor = SnowflakeExtractor(make_snowflake_config())
    extractor._streams_count_rows = True
    extractor._conn = MagicMock()
    extractor._conn.cursor = MagicMock()
    extractor._conn.cursor.return_value.__enter__.return_value = mock_row_count_cursor

    extractor._fetch_streams(mock_cursor, "DB", "SCHEMA")

    normalized_name = dataset_normalized_name("DB", "SCHEMA", "STREAM")
    assert normalized_name in extractor._datasets
    assert len(extractor._datasets) == 1

    dataset = extractor._datasets[normalized_name]
    assert dataset.schema.sql_schema.materialization is MaterializationType.STREAM
    assert dataset.statistics.record_count == 3
    assert dataset.snowflake_stream_info == SnowflakeStreamInfo(
        stream_type=SnowflakeStreamType.STANDARD,
        source_type=SnowflakeStreamSourceType.TABLE,
        stale=True,
        stale_after=datetime.fromisoformat("2023-01-01 12:00:00+00:00"),
    )


@patch("metaphor.snowflake.auth.connect")
def test_fetch_tags_override(mock_connect: MagicMock) -> None:
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            ("TEST_TAG", "db_tag", "DATABASE", None, None, "DEMO_DB", None),
            ("TEST_TAG", "schema_tag", "SCHEMA", "DEMO_DB", None, "METAPHOR", None),
            (
                "TEST_TAG",
                "table_tag",
                "TABLE",
                "DEMO_DB",
                "METAPHOR",
                "TABLE2",
                None,
            ),
            (
                "PRIVACY_CATEGORY",
                "SENSITIVE",
                "TABLE",
                "ACME",
                "RIDE_SHARE",
                "BIKES_AT_STATION",
                None,
            ),
            (
                "TEST_TAG",
                "column_3_tag",
                "COLUMN",
                "DEMO_DB",
                "METAPHOR",
                "TABLE2",
                "COL3",
            ),
            (
                "TEST_TAG",
                "column_2_tag",
                "COLUMN",
                "DEMO_DB",
                "METAPHOR",
                "TABLE1",
                "COL2",
            ),
        ]
    )

    extractor = SnowflakeExtractor(make_snowflake_config())

    for (schema, table), columns in {
        ("metaphor", "table1"): {"col1", "col2"},
        ("metaphor", "table2"): {"col3", "col4", "col5"},
        ("another", "table3"): {"col6"},
    }.items():
        dataset = extractor._init_dataset(
            "demo_db", schema, table, table_type, "", None, None
        )
        for column in columns:
            dataset.schema.fields.append(SchemaField(field_path=column, subfields=[]))
        extractor._datasets[dataset_normalized_name("demo_db", schema, table)] = dataset

    extractor._fetch_tags(mock_cursor)
    table1 = extractor._datasets.get("demo_db.metaphor.table1")
    table2 = extractor._datasets.get("demo_db.metaphor.table2")
    table3 = extractor._datasets.get("demo_db.another.table3")
    assert (
        table1
        and table1.system_tags
        and table1.system_tags.tags
        == [
            SystemTag(
                key="TEST_TAG",
                value="schema_tag",
                system_tag_source=SystemTagSource.SNOWFLAKE,
            )
        ]
    )
    assert (
        table2
        and table2.system_tags
        and table2.system_tags.tags
        == [
            SystemTag(
                key="TEST_TAG",
                value="table_tag",
                system_tag_source=SystemTagSource.SNOWFLAKE,
            )
        ]
    )
    assert (
        table3
        and table3.system_tags
        and table3.system_tags.tags
        == [
            SystemTag(
                key="TEST_TAG",
                value="db_tag",
                system_tag_source=SystemTagSource.SNOWFLAKE,
            )
        ]
    )

    assert table1.schema and table1.schema.fields
    for i in range(1, 3):
        field = next(
            (f for f in table1.schema.fields if f.field_path == f"col{i}"), None
        )
        assert field
        if i == 2:
            assert field.tags == ["TEST_TAG=column_2_tag"]
        else:
            assert field.tags == ["TEST_TAG=schema_tag"]

    assert table2.schema and table2.schema.fields
    for i in range(3, 6):
        field = next(
            (f for f in table2.schema.fields if f.field_path == f"col{i}"), None
        )
        assert field
        if i == 3:
            assert field.tags == ["TEST_TAG=column_3_tag"]
        else:
            assert field.tags == ["TEST_TAG=table_tag"]

    assert table3.schema and table3.schema.fields
    for i in range(6, 7):
        field = next(
            (f for f in table3.schema.fields if f.field_path == f"col{i}"), None
        )
        assert field
        assert field.tags == ["TEST_TAG=db_tag"]
