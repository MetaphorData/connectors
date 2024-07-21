from tests.common.sql.table_level_lineage.utils import assert_table_lineage_equal


def test_use():
    assert_table_lineage_equal("USE db1", None, None)


def test_drop():
    assert_table_lineage_equal("DROP TABLE IF EXISTS tab1", None, None)


def test_drop_with_comment():
    assert_table_lineage_equal(
        """--comment
DROP TABLE IF EXISTS tab1""",
        None,
        None,
    )


def test_drop_view():
    assert_table_lineage_equal("DROP VIEW IF EXISTS view1", None, None)


def test_truncate_table():
    assert_table_lineage_equal("TRUNCATE TABLE tab1", None, None)


def test_delete_from_table():
    assert_table_lineage_equal("DELETE FROM table tab1", None, None)
