import pytest

from metaphor.models.metadata_change_event import DataPlatform
from tests.common.sql.table_level_lineage.utils import assert_table_lineage_equal

"""
This test class will contain all the tests for testing 'Insert Queries' where the platform is not ANSI.
"""


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]
)  # TODO: sparksql
def test_insert_overwrite(platform: DataPlatform):
    assert_table_lineage_equal(
        "INSERT OVERWRITE TABLE tab1 SELECT col1 FROM tab2",
        {"tab2"},
        {"tab1"},
        platform=platform,
    )


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]
)  # TODO: sparksql
def test_insert_overwrite_from_self(platform: DataPlatform):
    assert_table_lineage_equal(
        """INSERT OVERWRITE TABLE foo
SELECT col FROM foo
WHERE flag IS NOT NULL""",
        {"foo"},
        {"foo"},
        platform=platform,
    )


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]
)  # TODO: sparksql
def test_insert_overwrite_from_self_with_join(platform: DataPlatform):
    assert_table_lineage_equal(
        """INSERT OVERWRITE TABLE tab_1
SELECT tab_2.col_a from tab_2
JOIN tab_1
ON tab_1.col_a = tab_2.cola""",
        {"tab_1", "tab_2"},
        {"tab_1"},
        platform=platform,
    )


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]
)  # TODO: sparksql
def test_insert_overwrite_values(platform: DataPlatform):
    assert_table_lineage_equal(
        "INSERT OVERWRITE TABLE tab1 VALUES ('val1', 'val2'), ('val3', 'val4')",
        None,
        {"tab1"},
        platform=platform,
    )


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]
)  # TODO: sparksql
def test_insert_into_with_keyword_table(platform: DataPlatform):
    assert_table_lineage_equal(
        "INSERT INTO TABLE tab1 VALUES (1, 2)",
        set(),
        {"tab1"},
        platform=platform,
    )


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]
)  # TODO: sparksql
def test_insert_into_partitions(platform: DataPlatform):
    assert_table_lineage_equal(
        "INSERT INTO TABLE tab1 PARTITION (par1=1) SELECT * FROM tab2",
        {"tab2"},
        {"tab1"},
        platform=platform,
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_insert_overwrite_without_table_keyword(platform: DataPlatform):
    assert_table_lineage_equal(
        "INSERT OVERWRITE tab1 SELECT * FROM tab2",
        {"tab2"},
        {"tab1"},
        platform=platform,
    )


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]
)  # TODO: sparksql
def test_lateral_view_using_json_tuple(platform: DataPlatform):
    sql = """INSERT OVERWRITE TABLE foo
SELECT sc.id, q.item0, q.item1
FROM bar sc
LATERAL VIEW json_tuple(sc.json, 'key1', 'key2') q AS item0, item1"""
    assert_table_lineage_equal(sql, {"bar"}, {"foo"}, platform)


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]
)  # TODO: sparksql
def test_lateral_view_outer(platform: DataPlatform):
    sql = """INSERT OVERWRITE TABLE foo
SELECT sc.id, q.col1
FROM bar sc
LATERAL VIEW OUTER explode(sc.json_array) q AS col1"""
    assert_table_lineage_equal(sql, {"bar"}, {"foo"}, platform)
