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
from databricks.sdk.service.catalog import TableType
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
from tests.test_utils import load_json


def dummy_config():
    return UnityCatalogRunConfig(
        host="http://dummy.host",
        token="",
        output=OutputConfig(),
    )


@patch("metaphor.unity_catalog.extractor.UnityCatalogExtractor.create_api")
@patch("metaphor.unity_catalog.extractor.list_table_lineage")
@patch("metaphor.unity_catalog.extractor.list_column_lineage")
@pytest.mark.asyncio
async def test_extractor(
    mock_list_column_lineage: MagicMock,
    mock_list_table_lineage: MagicMock,
    mock_create_api: MagicMock,
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
            ),
        ]

    def mock_list_query_history(include_metrics):
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

    mock_client = MagicMock()
    mock_client.catalogs = MagicMock()
    mock_client.catalogs.list = mock_list_catalogs
    mock_client.schemas = MagicMock()
    mock_client.schemas.list = mock_list_schemas
    mock_client.tables = MagicMock()
    mock_client.tables.list = mock_list_tables
    mock_client.query_history = MagicMock()
    mock_client.query_history.list = mock_list_query_history
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
                    fileInfo=FileInfo(path="s3://path", has_permission=True),
                ),
                LineageInfo(tableInfo=NoPermission(), fileInfo=None),
            ],
        ),
        TableLineage(),
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
        )
    ]
    mock_create_api.return_value = mock_client

    extractor = UnityCatalogExtractor(dummy_config())
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/unity_catalog/expected.json")
