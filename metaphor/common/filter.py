from fnmatch import fnmatch
from typing import Dict, Optional, Set

from pydantic.dataclasses import dataclass

TableFilter = Set[str]
SchemaFilter = Dict[str, Optional[TableFilter]]
DatabaseFilter = Dict[str, Optional[SchemaFilter]]


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

    def _accepted_by_schema_pattern(
        self,
        schema_name: str,
        table_name: str,
        schema_pattern: str,
        table_filter: Optional[TableFilter],
    ):
        if fnmatch(schema_name, schema_pattern):
            # None means all tables are accepted
            if table_filter is None:
                return True

            for table_pattern in table_filter:
                if fnmatch(table_name, table_pattern):
                    return True

        return False

    def _accepted_by_database_pattern(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        database_pattern: str,
        schema_filter: Optional[SchemaFilter],
    ):
        if fnmatch(database_name, database_pattern):
            # None means all schemas are accepted
            if schema_filter is None:
                return True

            for (pattern, table_filter) in schema_filter.items():
                if self._accepted_by_schema_pattern(
                    schema_name, table_name, pattern, table_filter
                ):
                    return True

        return False

    def _accepted_by_filter(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        database_filter: DatabaseFilter,
    ):
        for pattern, schema_filter in database_filter.items():
            if self._accepted_by_database_pattern(
                database_name, schema_name, table_name, pattern, schema_filter
            ):
                return True

        return False

    def include_table(self, database: str, schema: str, table: str) -> bool:
        database_lower = database.lower()
        schema_lower = schema.lower()
        table_lower = table.lower()

        # Filtered out by includes
        if self.includes is not None and not self._accepted_by_filter(
            database_lower, schema_lower, table_lower, self.includes
        ):
            return False

        # Filtered out by excludes
        if self.excludes is not None and self._accepted_by_filter(
            database_lower, schema_lower, table_lower, self.excludes
        ):
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
