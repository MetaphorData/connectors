from unittest.mock import MagicMock, patch

from metaphor.common.base_config import OutputConfig
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import EventUtil
from metaphor.common.filter import DatasetFilter
from metaphor.common.sampling import SamplingConfig
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    FieldStatistics,
)
from metaphor.snowflake.profile.config import (
    ColumnStatistics,
    SnowflakeProfileRunConfig,
)
from metaphor.snowflake.profile.extractor import SnowflakeProfileExtractor
from metaphor.snowflake.utils import DatasetInfo, SnowflakeTableType
from tests.test_utils import load_json


def column_statistics_config():
    return ColumnStatistics(
        null_count=True,
        unique_count=True,
        min_value=True,
        max_value=True,
        avg_value=True,
        std_dev=True,
    )


@patch("metaphor.snowflake.auth.connect")
def test_default_excludes(mock_connect: MagicMock):
    extractor = SnowflakeProfileExtractor(
        SnowflakeProfileRunConfig(
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


database = "db"
schema = "schema"
table_name = "table1"
column = "col1"
table_type = SnowflakeTableType.BASE_TABLE.value
normalized_name = dataset_normalized_name(database, schema, table_name)
table_info = DatasetInfo(
    database=database, schema=schema, name=table_name, type=table_type
)


@patch("metaphor.snowflake.auth.connect")
def test_fetch_tables(mock_connect: MagicMock):
    mock_cursor = MagicMock()

    mock_cursor.__iter__.return_value = iter(
        [
            (database, schema, table_name, table_type, "comment1", 10, 20000),
            (database, schema, table_name, SnowflakeTableType.VIEW.value, "", 0, 0),
        ]
    )

    extractor = SnowflakeProfileExtractor(
        SnowflakeProfileRunConfig(
            account="snowflake_account",
            user="user",
            password="password",
            filter=DatasetFilter(
                includes={database: None},
            ),
            output=OutputConfig(),
        )
    )

    extractor._fetch_tables(mock_cursor, database)

    assert len(extractor._datasets) == 1
    dataset = extractor._datasets[normalized_name]
    assert dataset.logical_id == DatasetLogicalID(
        name=normalized_name,
        platform=DataPlatform.SNOWFLAKE,
        account="snowflake_account",
    )

    # test excluded DB
    mock_cursor.__iter__.return_value = iter(
        [
            (schema, table_name, table_type, "comment1", 10, 20000),
        ]
    )
    extractor._fetch_tables(mock_cursor, "db2")

    # no new dataset added
    assert len(extractor._datasets) == 1


@patch("metaphor.snowflake.auth.connect")
@patch("metaphor.snowflake.utils.async_execute")
def test_simple_fetch_columns(mock_async_execute: MagicMock, mock_connect: MagicMock):
    mock_async_execute.return_value = {}

    extractor = SnowflakeProfileExtractor(
        SnowflakeProfileRunConfig(
            account="snowflake_account",
            user="user",
            password="password",
            output=OutputConfig(),
        )
    )

    connection = mock_connect()
    extractor._fetch_columns_async(connection, {normalized_name: table_info})

    assert len(extractor._datasets) == 0


@patch("metaphor.snowflake.auth.connect")
@patch("metaphor.snowflake.utils.async_execute")
def test_complex_fetch_columns(
    mock_async_execute: MagicMock, mock_connect: MagicMock, test_root_dir: str
):
    mock_async_execute.return_value = {
        "DEMO_DB.PUBLIC.BIRDS": [(4, 0, 0, 0)],
        "DEMO_DB.PUBLIC.BIRD_SIGHTINGS": [(5, 0, 0, 0, 0, 0, 0)],
        "DEMO_DB.PUBLIC.M3_CONSENSUS_AGGREGATE": [(0, 0, 0, 0)],
        "DEMO_DB.PUBLIC.SIGHTINGS": [(5, 0, 0, 0, 0)],
        "DEMO_DB.PUBLIC.T": [(2, 0, 0)],
        "DEMO_DB.PUBLIC.m3_consensus_aggregate": [(0, 0, 0, 0)],
    }

    class MockCursor:
        def execute(self, _query) -> None:
            pass

        def __iter__(self):
            rows = [
                (
                    "BIRDS",
                    "PUBLIC",
                    "ID",
                    '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "BIRDS",
                    "PUBLIC",
                    "COMMON",
                    '{"type":"TEXT","length":100,"byteLength":400,"nullable":true,"fixed":false}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "BIRDS",
                    "PUBLIC",
                    "CLASS",
                    '{"type":"TEXT","length":100,"byteLength":400,"nullable":true,"fixed":false}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "BIRDS_SIGHTINGS",
                    "PUBLIC",
                    "ID",
                    '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "BIRDS_SIGHTINGS",
                    "PUBLIC",
                    "COMMON_NAME",
                    '{"type":"TEXT","length":100,"byteLength":400,"nullable":true,"fixed":false}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "BIRDS_SIGHTINGS",
                    "PUBLIC",
                    "CLASSIFICATION",
                    '{"type":"TEXT","length":100,"byteLength":400,"nullable":true,"fixed":false}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "BIRDS_SIGHTINGS",
                    "PUBLIC",
                    "DATE",
                    '{"type":"DATE","nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "BIRDS_SIGHTINGS",
                    "PUBLIC",
                    "LOCATION",
                    '{"type":"TEXT","length":100,"byteLength":400,"nullable":true,"fixed":false}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "BIRDS_SIGHTINGS",
                    "PUBLIC",
                    "COUNT",
                    '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "M3_CONSENSUS_AGGREGATE",
                    "PUBLIC",
                    "ID",
                    '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "M3_CONSENSUS_AGGREGATE",
                    "PUBLIC",
                    "FOO",
                    '{"type":"TEXT","length":100,"byteLength":400,"nullable":true,"fixed":false}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "M3_CONSENSUS_AGGREGATE",
                    "PUBLIC",
                    "BAR",
                    '{"type":"TEXT","length":200,"byteLength":800,"nullable":true,"fixed":false}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "SIGHTINGS",
                    "PUBLIC",
                    "D",
                    '{"type":"DATE","nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "SIGHTINGS",
                    "PUBLIC",
                    "LOC",
                    '{"type":"TEXT","length":100,"byteLength":400,"nullable":true,"fixed":false}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "SIGHTINGS",
                    "PUBLIC",
                    "B_ID",
                    '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "SIGHTINGS",
                    "PUBLIC",
                    "C",
                    '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "T",
                    "PUBLIC",
                    "ID",
                    '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "T",
                    "PUBLIC",
                    "NAME",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "m3_consensus_aggregate",
                    "PUBLIC",
                    "ID",
                    '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "m3_consensus_aggregate",
                    "PUBLIC",
                    "FoO",
                    '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
                (
                    "m3_consensus_aggregate",
                    "PUBLIC",
                    "Phone Number",
                    '{"type":"TEXT","length":20,"byteLength":80,"nullable":true,"fixed":false}',
                    "",
                    "",
                    "",
                    "",
                    "",
                    "DEMO_DB",
                    "",
                ),
            ]
            for row in rows:
                yield row

    mock_connect.cursor = lambda: MockCursor()
    mock_raw_datasets = {
        "demo_db.public.birds": {
            "fieldStatistics": {"fieldStatistics": []},
            "logicalId": {
                "account": "metaphor-dev",
                "name": "demo_db.public.birds",
                "platform": "SNOWFLAKE",
            },
        },
        "demo_db.public.m3_consensus_aggregate": {
            "fieldStatistics": {"fieldStatistics": []},
            "logicalId": {
                "account": "metaphor-dev",
                "name": "demo_db.public.m3_consensus_aggregate",
                "platform": "SNOWFLAKE",
            },
        },
        "demo_db.public.sightings": {
            "fieldStatistics": {"fieldStatistics": []},
            "logicalId": {
                "account": "metaphor-dev",
                "name": "demo_db.public.sightings",
                "platform": "SNOWFLAKE",
            },
        },
        "demo_db.public.t": {
            "fieldStatistics": {"fieldStatistics": []},
            "logicalId": {
                "account": "metaphor-dev",
                "name": "demo_db.public.t",
                "platform": "SNOWFLAKE",
            },
        },
    }
    mock_datasets = {k: Dataset.from_dict(v) for k, v in mock_raw_datasets.items()}
    extractor = SnowflakeProfileExtractor(
        SnowflakeProfileRunConfig(
            account="snowflake_account",
            user="user",
            password="password",
            output=OutputConfig(),
        )
    )
    extractor._datasets = mock_datasets
    tables = {
        "demo_db.public.birds": DatasetInfo(
            database="demo_db",
            schema="PUBLIC",
            name="BIRDS",
            type="BASE TABLE",
            row_count=4,
        ),
        "demo_db.public.m3_consensus_aggregate": DatasetInfo(
            database="demo_db",
            schema="PUBLIC",
            name="m3_consensus_aggregate",
            type="BASE TABLE",
            row_count=0,
        ),
        "demo_db.public.sightings": DatasetInfo(
            database="demo_db",
            schema="PUBLIC",
            name="SIGHTINGS",
            type="BASE TABLE",
            row_count=5,
        ),
        "demo_db.public.t": DatasetInfo(
            database="demo_db",
            schema="PUBLIC",
            name="T",
            type="BASE TABLE",
            row_count=2,
        ),
    }
    extractor._fetch_columns_async(mock_connect, tables)

    events = [EventUtil.trim_event(e) for e in extractor._datasets.values()]

    assert events == load_json(f"{test_root_dir}/snowflake/profile/expected.json")


def test_build_profiling_query():
    columns = [
        ("id", "STRING"),
        ("price", "FLOAT"),
        ("year", "NUMBER"),
    ]
    schema, name = "schema", "table"

    expected = (
        "SELECT COUNT(1), "
        'COUNT(DISTINCT "id"), COUNT(1) - COUNT("id"), '
        'COUNT(DISTINCT "price"), COUNT(1) - COUNT("price"), MIN("price"), MAX("price"), AVG("price"), STDDEV(CAST("price" as DOUBLE)), '
        'COUNT(DISTINCT "year"), COUNT(1) - COUNT("year"), MIN("year"), MAX("year"), AVG("year"), STDDEV(CAST("year" as DOUBLE)) '
        'FROM "schema"."table"'
    )

    assert (
        SnowflakeProfileExtractor._build_profiling_query(
            columns, schema, name, 0, column_statistics_config(), SamplingConfig()
        )
        == expected
    )


def test_build_profiling_query_with_sampling():
    columns = [
        ("id", "STRING"),
        ("price", "FLOAT"),
    ]
    schema, name = "schema", "table"

    expected = (
        "SELECT COUNT(1), "
        'COUNT(DISTINCT "id"), COUNT(1) - COUNT("id"), '
        'COUNT(DISTINCT "price"), COUNT(1) - COUNT("price"), MIN("price"), MAX("price"), AVG("price"), STDDEV(CAST("price" as DOUBLE)) '
        'FROM "schema"."table" SAMPLE SYSTEM (1)'
    )

    assert (
        SnowflakeProfileExtractor._build_profiling_query(
            columns,
            schema,
            name,
            100000000,
            column_statistics_config(),
            SamplingConfig(percentage=1, threshold=100000000),
        )
        == expected
    )


def test_parse_profiling_result():
    columns = [
        ("id", "STRING"),
        ("price", "FLOAT"),
        ("year", "NUMBER"),
    ]
    results = (
        # row count
        10,
        # id
        1,
        2,
        # price
        3,
        4,
        5,
        6,
        7,
        8,
        # year
        9,
        10,
        11,
        float("inf"),
        13,
        float("NAN"),
    )
    dataset = SnowflakeProfileExtractor._init_dataset(
        account="a", normalized_name="foo"
    )

    SnowflakeProfileExtractor._parse_profiling_result(
        columns, results, dataset, column_statistics_config()
    )

    assert dataset == Dataset(
        field_statistics=DatasetFieldStatistics(
            field_statistics=[
                FieldStatistics(
                    distinct_value_count=1.0,
                    field_path="id",
                    nonnull_value_count=8.0,
                    null_value_count=2.0,
                ),
                FieldStatistics(
                    average=7.0,
                    distinct_value_count=3.0,
                    field_path="price",
                    max_value=6.0,
                    min_value=5.0,
                    nonnull_value_count=6.0,
                    null_value_count=4.0,
                    std_dev=8.0,
                ),
                FieldStatistics(
                    average=13.0,
                    distinct_value_count=9.0,
                    field_path="year",
                    max_value=None,
                    min_value=11.0,
                    nonnull_value_count=0.0,
                    null_value_count=10.0,
                    std_dev=None,
                ),
            ]
        ),
        logical_id=DatasetLogicalID(
            account="a", name="foo", platform=DataPlatform.SNOWFLAKE
        ),
    )
