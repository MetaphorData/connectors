import json
from unittest.mock import patch

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.snowflake.lineage.config import SnowflakeLineageRunConfig
from metaphor.snowflake.lineage.extractor import SnowflakeLineageExtractor
from tests.test_utils import load_json


def dummy_config():
    return SnowflakeLineageRunConfig(
        account="snowflake_account",
        user="user",
        password="password",
        output=OutputConfig(),
    )


def test_parse_access_log(test_root_dir):
    accessed_objects = json.dumps(
        [
            {
                "columns": [
                    {"columnId": 1, "columnName": "FOO"},
                    {"columnId": 2, "columnName": "BAR"},
                ],
                "objectDomain": "Table",
                "objectId": 3,
                "objectName": "DB1.SCHEMA1.TABLE1",
            },
            {
                "columns": [
                    {"columnName": "BAZ"},
                    {"columnName": "QUX"},
                ],
                "objectDomain": "View",
                "objectId": 6,
                "objectName": "DB1.SCHEMA1.TABLE2",
            },
        ]
    )

    modified_objects = json.dumps(
        [
            {
                "columns": [{"columnId": 7, "columnName": "FOO"}],
                "objectDomain": "TABLE",
                "objectId": 8,
                "objectName": "DB2.SCHEMA1.TABLE1",
            },
            {
                "columns": [{"columnId": 9, "columnName": "BAR"}],
                "objectDomain": "TABLE",
                "objectId": 10,
                "objectName": "DB2.SCHEMA1.TABLE2",
            },
        ]
    )

    with patch("metaphor.snowflake.auth.connect"):
        extractor = SnowflakeLineageExtractor(dummy_config())
        extractor._parse_access_log(accessed_objects, modified_objects, "query")

        results = {}
        for key, value in extractor._datasets.items():
            results[key] = EventUtil.clean_nones(value.to_dict())

        assert results == load_json(
            test_root_dir + "/snowflake/lineage/data/parse_query_log_result.json"
        )


def test_parse_object_dependencies(test_root_dir):
    dependencies = [
        ("ACME", "METAPHOR", "FOO", "TABLE", "ACME", "METAPHOR", "BAR", "VIEW"),
        ("ACME", "METAPHOR", "ABC", "TABLE", "ACME", "METAPHOR", "XYZ", "VIEW"),
        ("ACME", "METAPHOR", "F", "TABLE", "ACME", "METAPHOR", "B", "STAGE"),
    ]

    with patch("metaphor.snowflake.auth.connect"):
        extractor = SnowflakeLineageExtractor(dummy_config())
        extractor._parse_object_dependencies(dependencies)

        results = {}
        for key, value in extractor._datasets.items():
            results[key] = EventUtil.clean_nones(value.to_dict())

        assert results == load_json(
            test_root_dir
            + "/snowflake/lineage/data/parse_object_dependencies_result.json"
        )
