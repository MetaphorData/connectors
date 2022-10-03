from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.unity_catalog.config import UnityCatalogRunConfig
from metaphor.unity_catalog.extractor import UnityCatalogExtractor
from tests.test_utils import load_json


def dummy_config():
    return UnityCatalogRunConfig(
        host="",
        token="",
        output=OutputConfig(),
    )


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor(test_root_dir):
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
                    "table_type": "TYPE",
                    "data_source_format": "csv",
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
                }
            ]
        }

    with patch(
        "metaphor.unity_catalog.extractor.UnityCatalogExtractor.create_api"
    ) as mock_create_api:
        mock_client = MagicMock()
        mock_client.list_catalogs = mock_list_catalogs
        mock_client.list_schemas = mock_list_schemas
        mock_client.list_tables = mock_list_tables
        mock_create_api.return_value = mock_client

        extractor = UnityCatalogExtractor(dummy_config())
        events = [EventUtil.trim_event(e) for e in await extractor.extract()]

        import json

        print(json.dumps(events))

    assert events == load_json(f"{test_root_dir}/unity_catalog/expected.json")
