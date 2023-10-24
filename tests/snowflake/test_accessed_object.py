import json
from typing import List

from pydantic import TypeAdapter

from metaphor.models.metadata_change_event import QueriedDataset
from metaphor.snowflake import SnowflakeExtractor
from metaphor.snowflake.accessed_object import AccessedObject
from metaphor.snowflake.config import SnowflakeRunConfig


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

    ta = TypeAdapter(List[AccessedObject])
    result = ta.validate_python(data)
    assert len(result) == 3


def test_parse_access_logs(test_root_dir):
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

    config = SnowflakeRunConfig.from_yaml_file(f"{test_root_dir}/snowflake/config.yml")
    extractor = SnowflakeExtractor(config)
    result = extractor._parse_accessed_objects(json.dumps(data))
    assert result == [
        QueriedDataset(
            id="DATASET~3825E6820162DA51607B2362F5C3A89F",
            database="dev",
            schema="go",
            table="gators",
            columns=["BAR"],
        )
    ]
