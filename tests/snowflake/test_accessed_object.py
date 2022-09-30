import json
from typing import List

from pydantic import parse_raw_as

from metaphor.models.metadata_change_event import QueriedDataset
from metaphor.snowflake import SnowflakeExtractor
from metaphor.snowflake.accessed_object import AccessedObject


def test_pydantic_dataclass():
    data = [
        {
            "objectDomain": "Stage",
            "objectId": 1,
            "objectName": "DEV.SANDBOX.FOO",
            "stageKind": "Internal Named",
        },
        {
            "locations": [
                "s3://datawriterprd-us-east-1/ad46b6f2-3966-404d-8c4f-b1011db05aaa/"
            ]
        },
        {
            "columns": [{"columnId": 12, "columnName": "BAR"}],
            "objectDomain": "Table",
            "objectId": 111,
            "objectName": "DEV.GO.GATORS",
        },
    ]

    result = parse_raw_as(List[AccessedObject], json.dumps(data))
    assert len(result) == 3


def test_parse_access_logs():
    data = [
        {
            "objectDomain": "Stage",
            "objectId": 1,
            "objectName": "DEV.SANDBOX.FOO",
            "stageKind": "Internal Named",
        },
        {
            "columns": [{"columnId": 12, "columnName": "BAR"}],
            "objectDomain": "Table",
            "objectId": 111,
            "objectName": "DEV.GO.GATORS",
        },
    ]

    result = SnowflakeExtractor._parse_accessed_objects(json.dumps(data))
    assert result == [
        QueriedDataset(database="dev", schema="go", table="gators", columns=["BAR"])
    ]
