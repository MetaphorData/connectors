from fnmatch import fnmatch
from typing import Dict, Optional, Set

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig

TableFilter = Set[str]
SchemaFilter = Dict[str, Optional[TableFilter]]
DatabaseFilter = Dict[str, Optional[SchemaFilter]]

DUMMY_DATABASE_NAME = "dummy"


@dataclass(config=ConnectorConfig)
class TwoLevelDatasetFilter:
    # A list of schemas/tables to include
    includes: Optional[SchemaFilter] = None

    # A list of schemas/tables to exclude
    excludes: Optional[SchemaFilter] = None


@dataclass(config=ConnectorConfig)
class DatasetFilter:
    # A list of databases/schemas/tables to include
    includes: Optional[DatabaseFilter] = None

    # A list of databases/schemas/tables to exclude
    excludes: Optional[DatabaseFilter] = None

    def merge(self, filter: "DatasetFilter") -> "DatasetFilter":
        """Merge with another filter and return a shallow copy"""

        def _merge_table_filters(
            f1: Optional[TableFilter], f2: Optional[TableFilter]
        ) -> Optional[TableFilter]:
            return f1 if f2 is None else f2 if f1 is None else f1.union(f2)

        def _merge_schema_filters(
            f1: Optional[SchemaFilter], f2: Optional[SchemaFilter]
        ) -> Optional[SchemaFilter]:
            if f1 is None:
                return f2
            if f2 is None:
                return f1

            result = f1.copy()  # shallow copy of f1
            for key, val in f2.items():
                result[key] = _merge_table_filters(f1.get(key, None), val)

            return result

        def _merge_database_filters(
            f1: Optional[DatabaseFilter], f2: Optional[DatabaseFilter]
        ) -> Optional[DatabaseFilter]:
            """
            Merge two database filters, if same key, then merge the schema filters
            """
            if f1 is None:
                return f2
            if f2 is None:
                return f1

            result = f1.copy()  # shallow copy of f1
            for key, val in f2.items():
                result[key] = _merge_schema_filters(f1.get(key, None), val)

            return result

        return DatasetFilter(
            includes=_merge_database_filters(self.includes, filter.includes),
            excludes=_merge_database_filters(self.excludes, filter.excludes),
        )

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

    @staticmethod
    def from_two_level_dataset_filter(filter: TwoLevelDatasetFilter) -> "DatasetFilter":
        return DatasetFilter(
            includes={DUMMY_DATABASE_NAME: filter.includes},
            excludes=(
                {DUMMY_DATABASE_NAME: filter.excludes} if filter.excludes else None
            ),
        )

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

            for pattern, table_filter in schema_filter.items():
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

    def include_table_two_level(self, schema: str, table: str) -> bool:
        return self.include_table(DUMMY_DATABASE_NAME, schema, table)

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

    def include_schema_two_level(self, schema: str) -> bool:
        return self.include_schema(DUMMY_DATABASE_NAME, schema)

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

            for schema_pattern, table_filter in schema_filter.items():
                # got a match
                if fnmatch(schema_lower, schema_pattern):
                    # fully covered
                    if table_filter is None or len(table_filter) == 0:
                        return True
                    else:
                        return partial

            return False

        if self.includes is not None and not covered_by_filter(self.includes, True):
            return False

        # Filtered out by excludes
        if self.excludes is not None and covered_by_filter(self.excludes, False):
            return False

        return True

    def include_database(
        self,
        database_name: str,
    ) -> bool:
        database_lower = database_name.lower()

        if self.excludes is not None:
            for pattern, schema_filter in self.excludes.items():
                # Only exclude if the entire database is excluded
                if schema_filter is not None and len(schema_filter) > 0:
                    continue

                if fnmatch(database_lower, pattern):
                    return False

        if self.includes is not None:
            for pattern in self.includes:
                if fnmatch(database_lower, pattern):
                    return True

            # can't match any include patterns
            return False

        return True


@dataclass
class TopicFilter:
    includes: Optional[TableFilter] = None
    excludes: Optional[TableFilter] = None

    def merge(self, filter: "TopicFilter") -> "TopicFilter":
        """Merge with another filter and return a shallow copy"""

        def merge_filters(f1: Optional[TableFilter], f2: Optional[TableFilter]):
            return f1 if f2 is None else f2 if f1 is None else {*f1, *f2}

        return TopicFilter(
            includes=merge_filters(self.includes, filter.includes),
            excludes=merge_filters(self.excludes, filter.excludes),
        )

    def normalize(self) -> "TopicFilter":
        def normalize_table_filter(
            table_filter: Optional[TableFilter],
        ) -> Optional[TableFilter]:
            if table_filter is None:
                return None
            else:
                return set([v.lower() for v in table_filter])

        includes = (
            normalize_table_filter(self.includes) if self.includes is not None else None
        )
        excludes = (
            normalize_table_filter(self.excludes) if self.excludes is not None else None
        )

        return TopicFilter(includes=includes, excludes=excludes)

    def _accepted_by_filter(self, topic: str, table_filter: TableFilter):
        for pattern in table_filter:
            if fnmatch(topic, pattern):
                return True
        return False

    def include_topic(self, topic: str) -> bool:
        if self.includes is not None and not self._accepted_by_filter(
            topic, self.includes
        ):
            return False
        if self.excludes is not None and self._accepted_by_filter(topic, self.excludes):
            return False
        return True
