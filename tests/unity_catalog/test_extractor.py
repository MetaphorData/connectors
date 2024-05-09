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
from databricks.sdk.service.sql import QueryInfo, QueryMetrics

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.unity_catalog.config import UnityCatalogRunConfig
from metaphor.unity_catalog.extractor import UnityCatalogExtractor
from metaphor.unity_catalog.models import (
    ColumnLineage,
    FileInfo,
    LineageColumnInfo,
    LineageInfo,
    NoPermission,
    TableInfo,
    TableLineage,
)
from tests.test_utils import load_json, wrap_query_log_stream_to_event


def dummy_config():
    return UnityCatalogRunConfig(
        host="http://dummy.host",
        token="",
        output=OutputConfig(),
    )


@patch("metaphor.unity_catalog.extractor.create_connection")
@patch("metaphor.unity_catalog.extractor.create_api")
@patch("metaphor.unity_catalog.extractor.list_table_lineage")
@patch("metaphor.unity_catalog.extractor.list_column_lineage")
@pytest.mark.asyncio
async def test_extractor(
    mock_list_column_lineage: MagicMock,
    mock_list_table_lineage: MagicMock,
    mock_create_api: MagicMock,
    mock_create_connection: MagicMock,
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
                owner="foo@bar.com",
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
                owner="foo@bar.com",
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
                owner="foo@bar.com",
                comment="example",
                updated_at=0,
                updated_by="foo@bar.com",
                properties={
                    "delta.lastCommitTimestamp": "1664444422000",
                },
                created_at=0,
            ),
        ]

    def mock_list_query_history(
        filter_by,
        include_metrics,
        max_results,  # No pagination!
    ):
        return [
            QueryInfo(
                duration=1234,
                query_id="foo",
                metrics=QueryMetrics(
                    read_remote_bytes=1234,
                    write_remote_bytes=5678,
                    rows_produced_count=5566,
                    rows_read_count=9487,
                ),
                query_text="bogus query",
                user_name="uwu",
                query_start_time_ms=55667788,
            )
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
    mock_client.query_history = MagicMock()
    mock_client.query_history.list = mock_list_query_history
    mock_client.volumes = MagicMock()
    mock_client.volumes.list = mock_list_volumes
    mock_list_table_lineage.side_effect = [
        TableLineage(
            upstreams=[
                LineageInfo(
                    tableInfo=TableInfo(
                        name="upstream", catalog_name="db", schema_name="schema"
                    ),
                    fileInfo=None,
                ),
                LineageInfo(
                    tableInfo=TableInfo(
                        name="upstream2", catalog_name="db", schema_name="schema"
                    ),
                    fileInfo=None,
                ),
                LineageInfo(
                    tableInfo=None,
                    fileInfo=FileInfo(
                        path="s3://path",
                        has_permission=True,
                    ),
                ),
                LineageInfo(tableInfo=NoPermission(), fileInfo=None),
            ],
        ),
        TableLineage(),
        TableLineage(
            upstreams=[
                LineageInfo(
                    tableInfo=None,
                    fileInfo=FileInfo(
                        path="s3://path/input.csv",
                        securable_name="catalog2.schema.volume",
                        securable_type="VOLUME",
                        storage_location="s3://path",
                        has_permission=True,
                    ),
                ),
            ],
            downstreams=[
                LineageInfo(
                    tableInfo=None,
                    fileInfo=FileInfo(
                        path="s3://path/output.csv",
                        securable_name="catalog2.schema.volume",
                        securable_type="VOLUME",
                        storage_location="s3://path",
                        has_permission=True,
                    ),
                ),
            ],
        ),
    ]
    mock_list_column_lineage.side_effect = [
        ColumnLineage(
            upstream_cols=[
                LineageColumnInfo(
                    name="col1",
                    catalog_name="db",
                    schema_name="schema",
                    table_name="upstream",
                )
            ]
        ),
        ColumnLineage(upstream_cols=[]),
    ]
    mock_create_api.return_value = mock_client

    mock_cursor = MagicMock()
    mock_cursor.fetchall = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [
            ("/Volumes/catalog2/schema/volume", "volume", "100000", 1715273354000)
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
            ("catalog", "schema", "table", "col1", "col_tag", "col_value"),
            ("catalog", "schema", "table", "col1", "col_tag2", "tag_value_2"),
            ("does", "not", "exist", "also", "doesn't", "exist"),
        ],
    ]

    mock_cursor_ctx = MagicMock()
    mock_cursor_ctx.__enter__ = MagicMock()
    mock_cursor_ctx.__enter__.return_value = mock_cursor

    mock_connection = MagicMock()
    mock_connection.cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor_ctx

    mock_create_connection.return_value = mock_connection

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
        == "http://dummy.host/explore/data/db/schema/table"
    )
    assert (
        extractor._get_volume_source_url("db", "schema", "table")
        == "http://dummy.host/explore/data/volumes/db/schema/table"
    )

    # Manual override with escaped characters
    config.source_url = "http://metaphor.io/{catalog}/{schema}/{table}"
    extractor = UnityCatalogExtractor(config)
    assert (
        extractor._get_table_source_url("d b", "<schema>", "{table}")
        == "http://metaphor.io/d%20b/%3Cschema%3E/%7Btable%7D"
    )


@patch("metaphor.unity_catalog.extractor.create_connection")
@patch("metaphor.unity_catalog.extractor.create_api")
def test_init_invalid_dataset(
    mock_create_api: MagicMock,
    mock_create_connection: MagicMock,
    test_root_dir: str,
) -> None:
    mock_create_api.return_value = None
    mock_create_connection.return_value = None

    extractor = UnityCatalogExtractor.from_config_file(
        f"{test_root_dir}/unity_catalog/config.yml"
    )
    with pytest.raises(ValueError):
        extractor._init_dataset(
            Table(catalog_name="catalog", schema_name="schema", name="table")
        )
