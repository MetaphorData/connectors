from dataclasses import dataclass
from typing import Dict, Optional, Set, Union

from serde import deserialize

TableFilter = Set[str]
SchemaFilter = Dict[str, Union[None, TableFilter]]
DatabaseFilter = Dict[str, Union[None, SchemaFilter]]


@deserialize
@dataclass
class SnowflakeFilter:
    # A list of databases/schemas/tables to include
    includes: Optional[DatabaseFilter] = None

    # A list of databases/schemas/tables to exclude
    excludes: Optional[DatabaseFilter] = None

    def normalize(self) -> "SnowflakeFilter":
        def normalize_table_filter(
            table_filter: Optional[TableFilter],
        ) -> Optional[TableFilter]:
            if table_filter is None:
                return None
            else:
                return set([v.lower() for v in table_filter])

        def normalize_schema_filter(
            schema_filter: Optional[SchemaFilter],
        ) -> Optional[SchemaFilter]:
            if schema_filter is None:
                return None
            else:
                return {
                    k.lower(): normalize_table_filter(v)
                    for k, v in schema_filter.items()
                }

        includes = (
            {k.lower(): normalize_schema_filter(v) for k, v in self.includes.items()}
            if self.includes is not None
            else None
        )

        excludes = (
            {k.lower(): normalize_schema_filter(v) for k, v in self.excludes.items()}
            if self.excludes is not None
            else None
        )

        return SnowflakeFilter(includes=includes, excludes=excludes)
