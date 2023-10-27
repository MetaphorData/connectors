from metaphor.common.filter import DatasetFilter
from metaphor.redshift.utils import exclude_system_databases


def test_exclude_system_databases():
    filter = DatasetFilter(
        includes={"db1": {}},
        excludes={"db2": {"schema2": set()}},
    )

    assert exclude_system_databases(filter) == DatasetFilter(
        includes={"db1": {}},
        excludes={
            "db2": {"schema2": set()},
            "padb_harvest": None,
            "temp": None,
            "awsdatacatalog": None,
        },
    )
