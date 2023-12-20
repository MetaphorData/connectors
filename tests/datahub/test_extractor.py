from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.datahub.config import DatahubConfig
from metaphor.datahub.extractor import DatahubExtractor
from tests.test_utils import load_json


@pytest.mark.asyncio
@patch("gql.client.Client.execute")
@patch("requests.get")
async def test_extractor(
    mock_requests_get: MagicMock,
    mock_gql_client_execute: MagicMock,
    test_root_dir: str,
) -> None:
    class MockGetResponse(BaseModel):
        entities: List[Dict[str, str]]

        def json(self):
            return self.model_dump()

    mock_requests_get.return_value = MockGetResponse(
        entities=[
            {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,acme.berlin_bicycles.cycle_hire,PROD)",
            },
            {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:synapse,foo,PROD)",
            },
            {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:mssql,bar,PROD)",
            },
        ]
    )

    def mock_execute(_query, variable_values):
        if (
            variable_values["urn"]
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,acme.berlin_bicycles.cycle_hire,PROD)"
        ):
            return {
                "dataset": {
                    "properties": {
                        "description": "Just a simple dataset",
                    },
                    "name": "acme.berlin_bicycles.cycle_hire",
                    "platform": {
                        "name": "snowflake",
                        "properties": {"datasetNameDelimiter": "."},
                    },
                    "tags": {
                        "tags": [
                            {
                                "tag": {
                                    "properties": {
                                        "name": "foo_Tag",
                                        "description": "Foo!!!",
                                    }
                                }
                            },
                            {
                                "tag": {
                                    "properties": {
                                        "name": "barrrrrr",
                                        "description": "bar",
                                    }
                                }
                            },
                        ],
                    },
                    "ownership": {
                        "owners": [
                            {
                                "owner": {"properties": {"email": "jdoe@test.io"}},
                                "ownershipType": {"info": {"name": "Technical Owner"}},
                            }
                        ]
                    },
                    "schemaMetadata": {
                        "fields": [
                            {
                                "fieldPath": "rental_id",
                                "description": "Rental ID",
                            },
                            {
                                "fieldPath": "duration",
                                "description": "Duuuuuuuuuuu",
                            },
                            {"fieldPath": "bike_id", "description": None},
                            {"fieldPath": "end_date", "description": None},
                        ]
                    },
                }
            }
        if (
            variable_values["urn"]
            == "urn:li:dataset:(urn:li:dataPlatform:synapse,foo,PROD)"
        ):
            return {
                "dataset": {
                    "properties": {
                        "description": "foo",
                    },
                    "name": "foo",
                    "platform": {
                        "name": "synapse",
                        "properties": None,
                    },
                    "tags": None,
                    "ownership": None,
                    "schemaMetadata": None,
                }
            }
        return {
            "dataset": {
                "properties": None,
                "name": "bar",
                "platform": {
                    "name": "mssql",
                    "properties": None,
                },
                "tags": None,
                "ownership": {
                    "owners": [
                        {
                            "owner": {"properties": {"email": "jdoe@test.io"}},
                            "ownershipType": {"info": {"name": "Technical Owner"}},
                        }
                    ]
                },
                "schemaMetadata": None,
            }
        }

    mock_gql_client_execute.side_effect = mock_execute

    dummy_config = DatahubConfig(
        output=OutputConfig(),
        host="localhost",
        port=9002,
        token="notatoken",
        snowflake_account="test-dev",
    )
    extractor = DatahubExtractor(dummy_config)
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/datahub/expected.json")
