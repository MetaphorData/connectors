import pytest

from metaphor.models.metadata_change_event import DataPlatform
from tests.common.sql.table_level_lineage.utils import assert_table_lineage_equal

# UPDATE expression should view the target table as a source.


@pytest.mark.parametrize("platform", [DataPlatform.MYSQL])
def test_update_with_join(platform: DataPlatform):
    assert_table_lineage_equal(
        "UPDATE tab1 a INNER JOIN tab2 b ON a.col1=b.col1 SET a.col2=b.col2",
        {"tab1", "tab2"},
        {"tab1"},
        platform=platform,
    )
