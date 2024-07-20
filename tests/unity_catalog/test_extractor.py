from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    ColumnTypeName,
    DataSourceFormat,
    SchemaInfo,
)
from databricks.sdk.service.catalog import TableInfo as Table
from databricks.sdk.service.catalog import TableType, VolumeInfo, VolumeType
from pytest_snapshot.plugin import Snapshot

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.unity_catalog.config import UnityCatalogRunConfig
from metaphor.unity_catalog.extractor import UnityCatalogExtractor
from metaphor.unity_catalog.models import Column, ColumnLineage, TableLineage
from tests.test_utils import load_json, serialize_event, wrap_query_log_stream_to_event
from tests.unity_catalog.mocks import mock_sql_connection


def dummy_config():
    return UnityCatalogRunConfig(
        hostname="dummy.host",
        http_path="path",
        token="",
        output=OutputConfig(),
    )


@patch("metaphor.unity_catalog.extractor.get_last_refreshed_time")
@patch("metaphor.unity_catalog.extractor.create_connection")
@patch("metaphor.unity_catalog.extractor.create_api")
@patch("metaphor.unity_catalog.extractor.list_table_lineage")
@patch("metaphor.unity_catalog.extractor.list_column_lineage")
@patch("metaphor.unity_catalog.extractor.list_query_log")
@pytest.mark.asyncio
async def test_extractor(
    mock_list_query_log: MagicMock,
    mock_list_column_lineage: MagicMock,
    mock_list_table_lineage: MagicMock,
    mock_create_api: MagicMock,
    mock_create_connection: MagicMock,
    mock_get_last_refreshed_time: MagicMock,
    test_root_dir: str,
):

    def mock_list_catalogs():
        return [CatalogInfo(name="catalog")]

    def mock_list_schemas(catalog):
        return [SchemaInfo(name="schema")]

    def mock_list_tables(catalog, schema):
        return [
            Table(
                name="table",
                catalog_name="catalog",
                schema_name="schema",
                table_type=TableType.MANAGED,
                data_source_format=DataSourceFormat.CSV,
                columns=[
                    ColumnInfo(
                        name="col1",
                        type_name=ColumnTypeName.INT,
                        type_precision=32,
                        nullable=True,
                        comment="some description",
                    )
                ],
                storage_location="s3://path",
                owner="user1@foo.com",
                comment="example",
                updated_at=0,
                updated_by="foo@bar.com",
                properties={
                    "delta.lastCommitTimestamp": "1664444422000",
                },
                created_at=0,
            ),
            Table(
                name="view",
                catalog_name="catalog",
                schema_name="schema",
                table_type=TableType.VIEW,
                columns=[
                    ColumnInfo(
                        name="col1",
                        type_name=ColumnTypeName.INT,
                        type_precision=32,
                        nullable=True,
                    )
                ],
                view_definition="SELECT ...",
                owner="user2@foo.com",
                comment="example",
                updated_at=0,
                updated_by="foo@bar.com",
                properties={
                    "view.catalogAndNamespace.numParts": "2",
                    "view.sqlConfig.spark.sql.hive.convertCTAS": "true",
                    "view.query.out.col.0": "key",
                    "view.sqlConfig.spark.sql.parquet.compression.codec": "snappy",
                    "view.query.out.numCols": "3",
                    "view.referredTempViewNames": "[]",
                    "view.query.out.col.1": "values",
                    "view.sqlConfig.spark.sql.streaming.stopTimeout": "15s",
                    "view.catalogAndNamespace.part.0": "catalog",
                    "view.sqlConfig.spark.sql.sources.commitProtocolClass": "com.databricks.sql.transaction.directory.DirectoryAtomicCommitProtocol",
                    "view.sqlConfig.spark.sql.sources.default": "delta",
                    "view.sqlConfig.spark.sql.legacy.createHiveTableByDefault": "false",
                    "view.query.out.col.2": "nested_values",
                    "view.referredTempFunctionsNames": "[]",
                    "view.catalogAndNamespace.part.1": "default",
                },
                created_at=0,
            ),
            Table(
                name="table2",
                catalog_name="catalog2",
                schema_name="schema",
                table_type=TableType.MANAGED,
                data_source_format=DataSourceFormat.DELTA,
                columns=[
                    ColumnInfo(
                        name="col1",
                        type_name=ColumnTypeName.INT,
                        type_precision=32,
                        nullable=True,
                        comment="some description",
                    )
                ],
                storage_location="s3://path",
                owner="user3@foo.com",
                comment="example",
                updated_at=0,
                updated_by="foo@bar.com",
                properties={
                    "delta.lastCommitTimestamp": "1664444422000",
                },
                created_at=0,
            ),
        ]

    def mock_list_volumes(catalog, schema):
        return [
            VolumeInfo(
                access_point=None,
                catalog_name="catalog2",
                comment=None,
                created_at=1714034378658,
                created_by="foo@bar.com",
                encryption_details=None,
                full_name="catalog2.schema.volume",
                metastore_id="ashjkdhaskd",
                name="volume",
                owner="foo@bar.com",
                schema_name="schema",
                storage_location="s3://path",
                updated_at=1714034378658,
                updated_by="foo@bar.com",
                volume_id="volume-id",
                volume_type=VolumeType.EXTERNAL,
            )
        ]

    mock_client = MagicMock()
    mock_client.catalogs = MagicMock()
    mock_client.catalogs.list = mock_list_catalogs
    mock_client.schemas = MagicMock()
    mock_client.schemas.list = mock_list_schemas
    mock_client.tables = MagicMock()
    mock_client.tables.list = mock_list_tables
    mock_client.volumes = MagicMock()
    mock_client.volumes.list = mock_list_volumes
    mock_list_table_lineage.side_effect = [
        {
            "catalog.schema.table": TableLineage(
                upstream_tables=[
                    "db.schema.upstream",
                    "db.schema.upstream2",
                ],
            )
        }
    ]

    mock_list_column_lineage.side_effect = [
        {
            "catalog.schema.table": ColumnLineage(
                upstream_columns={
                    "col1": [
                        Column(column_name="col1", table_name="db.schema.upstream")
                    ]
                }
            ),
        }
    ]

    mock_list_query_log.return_value = [
        {
            "query_id": "foo",
            "email": "foo@bar.com",
            "start_time": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "duration": 1234,
            "rows_read": 9487,
            "rows_written": 5566,
            "bytes_read": 1234,
            "bytes_written": 5678,
            "query_type": "SELECT",
            "query_text": "bogus query",
            "sources": "[]",
            "targets": "[]",
        }
    ]

    mock_get_last_refreshed_time.return_value = datetime(
        2020, 1, 1, tzinfo=timezone.utc
    )

    mock_create_api.return_value = mock_client

    results = [
        [
            (
                "/Volumes/catalog2/schema/volume/input.csv",
                "input.csv",
                "100000",
                1715273354000,
            ),
            (
                "/Volumes/catalog2/schema/volume/output.csv",
                "output.csv",
                "200000",
                1715278354000,
            ),
        ],
        [
            ("catalog_tag_key_1", "catalog_tag_value_1"),
            ("catalog_tag_key_2", "catalog_tag_value_2"),
        ],
        [
            ("schema", "schema_tag_key_1", "schema_tag_value_1"),
            ("schema", "schema_tag_key_2", "schema_tag_value_2"),
        ],
        [
            ("catalog", "schema", "table", "tag", "value"),
            ("catalog", "schema", "table", "tag2", ""),
            ("does", "not", "exist", "also", "doesn't exist"),
        ],
        [
            ("catalog2", "schema", "volume", "tag", "value"),
        ],
        [
            ("catalog", "schema", "table", "col1", "col_tag", "col_value"),
            ("catalog", "schema", "table", "col1", "col_tag2", "tag_value_2"),
            ("does", "not", "exist", "also", "doesn't", "exist"),
        ],
    ]

    mock_create_connection.return_value = mock_sql_connection(results)

    extractor = UnityCatalogExtractor(dummy_config())
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    expected = f"{test_root_dir}/unity_catalog/expected.json"
    assert events == load_json(expected)

    query_logs = wrap_query_log_stream_to_event(extractor.collect_query_logs())
    expected_query_logs = f"{test_root_dir}/unity_catalog/query_logs.json"
    assert query_logs == load_json(expected_query_logs)


@patch("metaphor.unity_catalog.extractor.create_connection")
@patch("metaphor.unity_catalog.extractor.create_api")
def test_source_url(
    mock_create_api: MagicMock,
    mock_create_connection: MagicMock,
):
    mock_create_api.return_value = None
    mock_create_connection.return_value = None

    # Default pattern
    config = dummy_config()
    extractor = UnityCatalogExtractor(config)
    assert (
        extractor._get_table_source_url("db", "schema", "table")
        == "https://dummy.host/explore/data/db/schema/table"
    )
    assert (
        extractor._get_volume_source_url("db", "schema", "table")
        == "https://dummy.host/explore/data/volumes/db/schema/table"
    )

    # Manual override with escaped characters
    config.source_url = "http://metaphor.io/{catalog}/{schema}/{table}"
    extractor = UnityCatalogExtractor(config)
    assert (
        extractor._get_table_source_url("d b", "<schema>", "{table}")
        == "http://metaphor.io/d%20b/%3Cschema%3E/%7Btable%7D"
    )

    assert (
        extractor._get_location_url("foo")
        == "https://dummy.host/explore/location/foo/browse"
    )


@patch("metaphor.unity_catalog.extractor.get_last_refreshed_time")
@patch("metaphor.unity_catalog.extractor.create_connection")
@patch("metaphor.unity_catalog.extractor.create_api")
def test_init_invalid_dataset(
    mock_create_api: MagicMock,
    mock_create_connection: MagicMock,
    mock_get_last_refreshed_time: MagicMock,
    test_root_dir: str,
) -> None:
    mock_create_api.return_value = None
    mock_create_connection.return_value = None
    mock_get_last_refreshed_time.return_value = datetime(2020, 1, 1)

    extractor = UnityCatalogExtractor.from_config_file(
        f"{test_root_dir}/unity_catalog/config.yml"
    )
    with pytest.raises(ValueError):
        extractor._init_dataset(
            Table(catalog_name="catalog", schema_name="schema", name="table")
        )


@patch("metaphor.unity_catalog.extractor.get_last_refreshed_time")
@patch("metaphor.unity_catalog.extractor.create_connection")
@patch("metaphor.unity_catalog.extractor.create_api")
def test_init_dataset(
    mock_create_api: MagicMock,
    mock_create_connection: MagicMock,
    mock_get_last_refreshed_time: MagicMock,
    test_root_dir: str,
    snapshot: Snapshot,
) -> None:
    mock_create_api.return_value = None
    mock_create_connection.return_value = None
    mock_get_last_refreshed_time.return_value = datetime(
        2020, 1, 1, tzinfo=timezone.utc
    )

    extractor = UnityCatalogExtractor.from_config_file(
        f"{test_root_dir}/unity_catalog/config.yml"
    )

    snapshot.assert_match(
        serialize_event(
            extractor._init_dataset(
                Table(
                    name="table",
                    catalog_name="catalog",
                    schema_name="schema",
                    table_type=TableType.MANAGED,
                    data_source_format=DataSourceFormat.CSV,
                    columns=[
                        ColumnInfo(
                            name="col1",
                            type_name=ColumnTypeName.INT,
                            type_precision=32,
                            nullable=True,
                            comment="some description",
                        )
                    ],
                    storage_location="s3://path",
                    owner="foo@bar.com",
                    comment="example",
                    updated_at=0,
                    updated_by="foo@bar.com",
                    properties={
                        "delta.lastCommitTimestamp": "1664444422000",
                    },
                    created_at=0,
                ),
            )
        ),
        "table.json",
    )

    snapshot.assert_match(
        serialize_event(
            extractor._init_dataset(
                Table(
                    name="table",
                    catalog_name="catalog",
                    schema_name="schema",
                    table_type=TableType.MANAGED_SHALLOW_CLONE,
                ),
            )
        ),
        "shallow_clone.json",
    )

    snapshot.assert_match(
        serialize_event(
            extractor._init_dataset(
                Table(
                    name="table",
                    catalog_name="catalog",
                    schema_name="schema",
                    table_type=TableType.EXTERNAL_SHALLOW_CLONE,
                ),
            )
        ),
        "external_shallow_clone.json",
    )
