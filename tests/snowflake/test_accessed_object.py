import json
from typing import List

from pydantic import parse_raw_as

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
