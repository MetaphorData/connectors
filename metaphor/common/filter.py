from typing import Dict, Optional, Set, Union

from pydantic.dataclasses import dataclass

TableFilter = Set[str]
SchemaFilter = Dict[str, Union[None, TableFilter]]
DatabaseFilter = Dict[str, Union[None, SchemaFilter]]


@dataclass
class DatasetFilter:
    # A list of databases/schemas/tables to include
    includes: Optional[DatabaseFilter] = None

    # A list of databases/schemas/tables to exclude
    excludes: Optional[DatabaseFilter] = None

    def normalize(self) -> "DatasetFilter":
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

        return DatasetFilter(includes=includes, excludes=excludes)

    def include_table(self, database: str, schema: str, table: str) -> bool:
        database_lower = database.lower()
        schema_lower = schema.lower()
        table_lower = table.lower()

        def covered_by_filter(database_filter: DatabaseFilter):
            # not covered by database filter
            if database_lower not in database_filter:
                return False

            schema_filter = database_filter[database_lower]

            # empty schema filter
            if schema_filter is None or len(schema_filter) == 0:
                return True

            # not covered by schema filter
            if schema_lower not in schema_filter:
                return False

            table_filter = schema_filter[schema_lower]

            # empty table filter
            if table_filter is None or len(table_filter) == 0:
                return True

            # covered by table filter?
            return table_lower in table_filter

        # Filtered out by includes
        if self.includes is not None and not covered_by_filter(self.includes):
            return False

        # Filtered out by excludes
        if self.excludes is not None and covered_by_filter(self.excludes):
            return False

        return True

    def include_schema(self, database: str, schema: str) -> bool:
        database_lower = database.lower()
        schema_lower = schema.lower()

        def covered_by_filter(database_filter: DatabaseFilter, partial: bool):
            if database_lower not in database_filter:
                return False

            schema_filter = database_filter[database_lower]

            # empty schema filter
            if schema_filter is None or len(schema_filter) == 0:
                return True

            if schema_lower in schema_filter:
                table_filter = schema_filter[schema_lower]

                # fully covered
                if table_filter is None or len(table_filter) == 0:
                    return True

                return partial

            return False

        if self.includes is not None and not covered_by_filter(self.includes, True):
            return False

        # Filtered out by excludes
        if self.excludes is not None and covered_by_filter(self.excludes, False):
            return False

        return True
