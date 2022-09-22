from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.glue.config import AwsCredentials, GlueRunConfig
from metaphor.glue.extractor import GlueExtractor
from tests.test_utils import load_json


def dummy_config():
    return GlueRunConfig(
        aws=AwsCredentials(
            access_key_id="key", secret_access_key="secret", region_name="region"
        ),
        output=OutputConfig(),
    )


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor(test_root_dir):
    databases = [
        {
            "DatabaseList": [
                {
                    "Name": "db1",
                },
                {
                    "Name": "db2",
                },
            ]
        }
    ]

    db1_tables = [
        {
            "TableList": [
                {
                    "Name": "table1",
                    "UpdateTime": datetime(2022, 9, 20, 8, 30, 0, 0),
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "col1", "Type": "int", "Comment": "description"}
                        ],
                        "Location": "s3://path",
                    },
                    "Parameters": {
                        "numRows": 100000,
                    },
                    "Description": "Table description",
                }
            ]
        }
    ]

    db2_tables = [
        {
            "TableList": [
                {
                    "Name": "table2",
                    "UpdateTime": datetime(2022, 9, 20, 8, 30, 0, 0),
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "col3", "Type": "int", "Comment": "description"}
                        ],
                        "Location": "s3://path",
                    },
                    "Parameters": {
                        "numRows": 100000,
                    },
                    "Description": "Table description",
                }
            ]
        }
    ]

    def mock_tables(DatabaseName: str):
        if DatabaseName == "db1":
            return db1_tables
        else:
            return db2_tables

    def mock_get_paginator(method: str):
        if method == "get_databases":
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = databases
            return mock_paginator
        elif method == "get_tables":
            mock_paginator = MagicMock()
            mock_paginator.paginate = mock_tables
            return mock_paginator

    with patch("metaphor.glue.extractor.create_glue_client") as mock_create_client:
        mock_client = MagicMock()
        mock_client.get_paginator = mock_get_paginator
        mock_create_client.return_value = mock_client

        extractor = GlueExtractor(dummy_config())
        events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/glue/expected.json")
