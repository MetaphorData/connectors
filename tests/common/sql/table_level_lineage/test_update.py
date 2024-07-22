from tests.common.sql.table_level_lineage.utils import assert_table_lineage_equal

# UPDATE expression should view the target table as a source.


def test_update():
    assert_table_lineage_equal(
        "UPDATE tab1 SET col1='val1' WHERE col2='val2'", {"tab1"}, {"tab1"}
    )


def test_update_from():
    assert_table_lineage_equal(
        """UPDATE tab2
SET tab2.col2 = tab1.col2 FROM tab1
WHERE tab2.col1 = tab1.col1""",
        {"tab1", "tab2"},
        {"tab2"},
    )
