from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.iam import ServicePrincipal
from pytest_snapshot.plugin import Snapshot

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.models.metadata_change_event import DataPlatform, QueryLog
from metaphor.unity_catalog.config import UnityCatalogRunConfig
from metaphor.unity_catalog.extractor import UnityCatalogExtractor
from metaphor.unity_catalog.models import (
    CatalogInfo,
    Column,
    ColumnInfo,
    ColumnLineage,
    SchemaInfo,
    TableInfo,
    TableLineage,
    Tag,
    VolumeFileInfo,
    VolumeInfo,
)
from tests.test_utils import load_json, serialize_event, wrap_query_log_stream_to_event


def dummy_config():
    return UnityCatalogRunConfig(
        hostname="dummy.host",
        http_path="path",
        token="",
        output=OutputConfig(),
    )


mock_time = datetime(2020, 1, 1, tzinfo=timezone.utc)


@patch("metaphor.unity_catalog.extractor.batch_get_last_refreshed_time")
@patch("metaphor.unity_catalog.extractor.create_connection_pool")
@patch("metaphor.unity_catalog.extractor.create_connection")
@patch("metaphor.unity_catalog.extractor.create_api")
@patch("metaphor.unity_catalog.extractor.list_catalogs")
@patch("metaphor.unity_catalog.extractor.list_schemas")
@patch("metaphor.unity_catalog.extractor.list_tables")
@patch("metaphor.unity_catalog.extractor.list_volumes")
@patch("metaphor.unity_catalog.extractor.list_volume_files")
@patch("metaphor.unity_catalog.extractor.list_table_lineage")
@patch("metaphor.unity_catalog.extractor.list_column_lineage")
@patch("metaphor.unity_catalog.extractor.batch_get_table_properties")
@patch("metaphor.unity_catalog.extractor.get_query_logs")
@patch("metaphor.unity_catalog.extractor.list_service_principals")
@pytest.mark.asyncio
async def test_extractor(
    mock_list_service_principals: MagicMock,
    mock_get_query_logs: MagicMock,
    mock_batch_get_table_properties: MagicMock,
    mock_list_column_lineage: MagicMock,
    mock_list_table_lineage: MagicMock,
    mock_list_volume_files: MagicMock,
    mock_list_volumes: MagicMock,
    mock_list_tables: MagicMock,
    mock_list_schemas: MagicMock,
    mock_list_catalogs: MagicMock,
    mock_create_api: MagicMock,
    mock_create_connection: MagicMock,
    mock_create_connection_pool: MagicMock,
    mock_batch_get_last_refreshed_time: MagicMock,
    test_root_dir: str,
):
    mock_list_service_principals.return_value = {
        "sp1": ServicePrincipal(display_name="service principal 1")
    }

    mock_list_catalogs.side_effect = [
        [
            CatalogInfo(
                catalog_name="catalog",
                owner="sp1",
                tags=[
                    Tag(key="catalog_tag_key_1", value="catalog_tag_value_1"),
                    Tag(key="catalog_tag_key_2", value="catalog_tag_value_2"),
                ],
            )
        ]
    ]
    mock_list_schemas.side_effect = [
        [
            SchemaInfo(
                catalog_name="catalog",
                schema_name="schema",
                owner="test@foo.bar",
                tags=[
                    Tag(key="schema_tag_key_1", value="schema_tag_value_1"),
                    Tag(key="schema_tag_key_2", value="schema_tag_value_2"),
                ],
            )
        ]
    ]

    mock_list_tables.side_effect = [
        [
            TableInfo(
                table_name="table",
                catalog_name="catalog",
                schema_name="schema",
                type="MANAGED",
                data_source_format="CSV",
                columns=[
                    ColumnInfo(
                        column_name="col1",
                        data_type="INT",
                        data_precision=32,
                        is_nullable=True,
                        comment="some description",
                        tags=[
                            Tag(key="col_tag", value="col_value"),
                            Tag(key="col_tag2", value="tag_value_2"),
                        ],
                    )
                ],
                storage_location="s3://path",
                owner="user1@foo.com",
                comment="example",
                updated_at=mock_time,
                updated_by="foo@bar.com",
                created_at=mock_time,
                created_by="foo@bar.com",
                tags=[
                    Tag(key="tag", value="value"),
                    Tag(key="tag2", value="value2"),
                ],
            ),
            TableInfo(
                table_name="view",
                catalog_name="catalog",
                schema_name="schema",
                type="VIEW",
                data_source_format="CSV",
                columns=[
                    ColumnInfo(
                        column_name="col1",
                        data_type="INT",
                        data_precision=32,
                        is_nullable=True,
                        tags=[],
                    )
                ],
                view_definition="SELECT ...",
                owner="user2@foo.com",
                comment="example",
                updated_at=mock_time,
                updated_by="foo@bar.com",
                created_at=mock_time,
                created_by="foo@bar.com",
            ),
            TableInfo(
                table_name="table2",
                catalog_name="catalog2",
                schema_name="schema",
                type="MANAGED",
                data_source_format="DELTA",
                columns=[
                    ColumnInfo(
                        column_name="col1",
                        data_type="INT",
                        data_precision=32,
                        is_nullable=True,
                        comment="some description",
                        tags=[],
                    )
                ],
                storage_location="s3://path",
                owner="sp1",
                comment="example",
                updated_at=mock_time,
                updated_by="foo@bar.com",
                created_at=mock_time,
                created_by="foo@bar.com",
            ),
        ]
    ]

    mock_list_volumes.side_effect = [
        [
            VolumeInfo(
                catalog_name="catalog2",
                schema_name="schema",
                volume_name="volume",
                volume_type="EXTERNAL",
                full_name="catalog2.schema.volume",
                owner="foo@bar.com",
                created_at=mock_time,
                created_by="foo@bar.com",
                updated_at=mock_time,
                updated_by="foo@bar.com",
                storage_location="s3://path",
                tags=[
                    Tag(key="tag", value="value"),
                ],
            )
        ]
    ]

    mock_list_volume_files.side_effect = [
        [
            VolumeFileInfo(
                last_updated=mock_time,
                name="input.csv",
                path="/Volumes/catalog2/schema/volume/input.csv",
                size=100000,
            ),
            VolumeFileInfo(
                last_updated=mock_time,
                name="output.csv",
                path="/Volumes/catalog2/schema/volume/output.csv",
                size=200000,
            ),
        ]
    ]

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

    mock_get_query_logs.return_value = [
        QueryLog(
            query_id="foo",
            email="foo@bar.com",
            start_time=mock_time,
            duration=1234.0,
            rows_read=9487.0,
            rows_written=5566.0,
            bytes_read=1234.0,
            bytes_written=5678.0,
            sql="bogus query",
            sources=[],
            targets=[],
            sql_hash="4d562df2c6dc30bee38ccfac33bc47d7",
            platform=DataPlatform.UNITY_CATALOG,
            id="UNITY_CATALOG:foo",
        )
    ]

    mock_batch_get_table_properties.return_value = {
        "catalog.schema.table": {
            "delta.lastCommitTimestamp": "1664444422000",
        },
        "catalog.schema.view": {
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
            "view.catalogAndNamespace.part.1": "default",
        },
    }

    mock_batch_get_last_refreshed_time.return_value = {
        "catalog.schema.table": mock_time,
        "catalog2.schema.table2": mock_time,
    }

    mock_create_api.return_value = MagicMock()

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


@patch("metaphor.unity_catalog.extractor.create_connection")
@patch("metaphor.unity_catalog.extractor.create_api")
def test_init_dataset(
    mock_create_api: MagicMock,
    mock_create_connection: MagicMock,
    test_root_dir: str,
    snapshot: Snapshot,
) -> None:
    mock_create_api.return_value = None
    mock_create_connection.return_value = None

    extractor = UnityCatalogExtractor.from_config_file(
        f"{test_root_dir}/unity_catalog/config.yml"
    )

    snapshot.assert_match(
        serialize_event(
            extractor._init_dataset(
                TableInfo(
                    table_name="table",
                    catalog_name="catalog",
                    schema_name="schema",
                    type="MANAGED",
                    data_source_format="csv",
                    columns=[
                        ColumnInfo(
                            column_name="col1",
                            data_type="INT",
                            data_precision=32,
                            is_nullable=True,
                            comment="some description",
                            tags=[
                                Tag(key="col1_tag_key_1", value="col1_tag_value_1"),
                                Tag(key="col1_tag_key_2", value="col1_tag_value_2"),
                            ],
                        )
                    ],
                    storage_location="s3://path",
                    owner="foo@bar.com",
                    comment="example",
                    updated_at=mock_time,
                    updated_by="foo@bar.com",
                    properties={
                        "delta.lastCommitTimestamp": "1664444422000",
                    },
                    created_at=mock_time,
                    created_by="foo@bar.com",
                ),
            )
        ),
        "table.json",
    )

    snapshot.assert_match(
        serialize_event(
            extractor._init_dataset(
                TableInfo(
                    table_name="table",
                    catalog_name="catalog",
                    schema_name="schema",
                    type="MANAGED_SHALLOW_CLONE",
                    owner="foo",
                    created_at=mock_time,
                    created_by="bar",
                    updated_at=mock_time,
                    updated_by="baz",
                    data_source_format="csv",
                ),
            )
        ),
        "shallow_clone.json",
    )

    snapshot.assert_match(
        serialize_event(
            extractor._init_dataset(
                TableInfo(
                    table_name="table",
                    catalog_name="catalog",
                    schema_name="schema",
                    type="EXTERNAL_SHALLOW_CLONE",
                    owner="foo",
                    created_at=mock_time,
                    created_by="bar",
                    updated_at=mock_time,
                    updated_by="baz",
                    data_source_format="csv",
                ),
            )
        ),
        "external_shallow_clone.json",
    )
