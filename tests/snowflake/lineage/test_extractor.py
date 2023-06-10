import json
from typing import Any, List, Tuple
from unittest.mock import MagicMock, patch

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.lineage.config import SnowflakeLineageRunConfig
from metaphor.snowflake.lineage.extractor import SnowflakeLineageExtractor
from tests.test_utils import load_json


def dummy_config(**args):
    return SnowflakeLineageRunConfig(
        account="snowflake_account",
        user="user",
        password="password",
        output=OutputConfig(),
        **args,
    )


@patch("metaphor.snowflake.auth.connect")
def test_default_excludes(mock_connect: MagicMock):
    extractor = SnowflakeLineageExtractor(
        SnowflakeLineageRunConfig(
            account="snowflake_account",
            user="user",
            password="password",
            filter=DatasetFilter(
                includes={"foo": None},
                excludes={"bar": None},
            ),
            output=OutputConfig(),
        )
    )

    assert extractor._filter.includes == {"foo": None}
    assert extractor._filter.excludes == {
        "bar": None,
        "SNOWFLAKE": None,
        "*": {"INFORMATION_SCHEMA": None},
    }


@patch("metaphor.snowflake.auth.connect")
def test_parse_access_log(mock_connect: MagicMock, test_root_dir: str):
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
            # self lineage
            {
                "columns": [
                    {"columnName": "BAZ"},
                    {"columnName": "QUX"},
                ],
                "objectDomain": "Table",
                "objectId": 6,
                "objectName": "DB2.SCHEMA1.TABLE1",
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

    # Include self-lineage
    extractor = SnowflakeLineageExtractor(dummy_config())
    extractor._parse_access_log(accessed_objects, modified_objects, "query")

    results = {}
    for key, value in extractor._datasets.items():
        results[key] = EventUtil.clean_nones(value.to_dict())

    assert results == load_json(
        test_root_dir
        + "/snowflake/lineage/data/parse_query_log_result_include_self_lineage.json"
    )

    # Exclude self-lineage
    extractor = SnowflakeLineageExtractor(dummy_config(include_self_lineage=False))
    extractor._parse_access_log(accessed_objects, modified_objects, "query")

    results = {}
    for key, value in extractor._datasets.items():
        results[key] = EventUtil.clean_nones(value.to_dict())

    assert results == load_json(
        test_root_dir
        + "/snowflake/lineage/data/parse_query_log_result_exclude_self_lineage.json"
    )


@patch("metaphor.snowflake.auth.connect")
def test_parse_object_dependencies(mock_connect: MagicMock, test_root_dir: str):
    dependencies: List[Tuple[Any, ...]] = [
        ("ACME", "METAPHOR", "FOO", "TABLE", "ACME", "METAPHOR", "BAR", "VIEW"),
        ("ACME", "METAPHOR", "ABC", "TABLE", "ACME", "METAPHOR", "XYZ", "VIEW"),
        ("ACME", "METAPHOR", "F", "TABLE", "ACME", "METAPHOR", "B", "STAGE"),
        # OBJECT_DEPENDENCIES can contain repeated rows
        ("ACME", "METAPHOR", "FOO", "TABLE", "ACME", "METAPHOR", "BAR", "VIEW"),
    ]

    extractor = SnowflakeLineageExtractor(dummy_config())
    extractor._parse_object_dependencies(dependencies)

    results = {}
    for key, value in extractor._datasets.items():
        results[key] = EventUtil.clean_nones(value.to_dict())

    assert results == load_json(
        test_root_dir + "/snowflake/lineage/data/parse_object_dependencies_result.json"
    )
