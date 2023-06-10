from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.unity_catalog.config import UnityCatalogRunConfig
from metaphor.unity_catalog.extractor import UnityCatalogExtractor
from metaphor.unity_catalog.models import (
    ColumnLineage,
    LineageColumnInfo,
    LineageInfo,
    TableInfo,
    TableLineage,
)
from tests.test_utils import load_json


def dummy_config():
    return UnityCatalogRunConfig(
        host="",
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
        return {"catalogs": [{"name": "catalog"}]}

    def mock_list_schemas(catalog_name, name_pattern):
        return {"schemas": [{"name": "schema"}]}

    def mock_list_tables(catalog_name, schema_name, name_pattern):
        return {
            "tables": [
                {
                    "name": "table",
                    "catalog_name": "catalog",
                    "schema_name": "schema",
                    "table_type": "MANAGED",
                    "data_source_format": "CSV",
                    "columns": [
                        {
                            "name": "col1",
                            "type_name": "int",
                            "type_precision": 32,
                            "nullable": True,
                        }
                    ],
                    "storage_location": "s3://path",
                    "owner": "foo@bar.com",
                    "comment": "example",
                    "updated_at": 0,
                    "updated_by": "foo@bar.com",
                    "properties": {
                        "delta.lastCommitTimestamp": "1664444422000",
                    },
                    "generation": 0,
                },
                {
                    "name": "view",
                    "catalog_name": "catalog",
                    "schema_name": "schema",
                    "table_type": "VIEW",
                    "columns": [
                        {
                            "name": "col1",
                            "type_name": "int",
                            "type_precision": 32,
                            "nullable": True,
                        }
                    ],
                    "view_definition": "SELECT ...",
                    "owner": "foo@bar.com",
                    "comment": "example",
                    "updated_at": 0,
                    "updated_by": "foo@bar.com",
                    "properties": {
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
                    "generation": 1,
                },
            ]
        }

    mock_client = MagicMock()
    mock_client.list_catalogs = mock_list_catalogs
    mock_client.list_schemas = mock_list_schemas
    mock_client.list_tables = mock_list_tables
    mock_list_table_lineage.side_effect = [
        TableLineage(
            upstreams=[
                LineageInfo(
                    tableInfo=TableInfo(
                        name="upstream", catalog_name="db", schema_name="schema"
                    )
                )
            ]
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
