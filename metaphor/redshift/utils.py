from copy import deepcopy

from metaphor.common.filter import DatasetFilter

IGNORED_DATABASES = ["padb_harvest", "temp", "awsdatacatalog", "sys:internal"]


def exclude_system_databases(filter: DatasetFilter) -> DatasetFilter:
    new_filter = deepcopy(filter)
    if new_filter.excludes is None:
        new_filter.excludes = {}

    for db in IGNORED_DATABASES:
        new_filter.excludes[db] = None

    return new_filter
