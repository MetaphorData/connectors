import pytest

from metaphor.models.metadata_change_event import DataPlatform
from tests.common.sql.table_level_lineage.utils import assert_table_lineage_equal


@pytest.mark.parametrize("platform", [DataPlatform.BIGQUERY])
def test_merge_without_into(platform: DataPlatform):
    """
    INTO is optional in BigQuery MERGE statement:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
    """
    sql = """MERGE target USING src AS b ON target.k = b.k
WHEN MATCHED THEN UPDATE SET target.v = b.v
WHEN NOT MATCHED THEN INSERT (k, v) VALUES (b.k, b.v)"""
    assert_table_lineage_equal(sql, {"src"}, {"target"}, platform=platform)


@pytest.mark.parametrize("platform", [DataPlatform.BIGQUERY])
def test_merge_insert_row(platform: DataPlatform):
    """
    MERGE INSERT CLAUSE in BigQuery can be INSERT ROW without specifying columns via INSERT VALUES (col, ...)
    https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
    """
    sql = """MERGE INTO tgt t
USING src s
ON t.date = s.date and t.channel = s.channel
WHEN NOT MATCHED THEN
INSERT ROW
WHEN MATCHED THEN
UPDATE SET t.col = s.col"""
    assert_table_lineage_equal(
        sql,
        {"src"},
        {"tgt"},
        platform=platform,
    )
