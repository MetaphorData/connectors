import typing
from datetime import datetime
from typing import Callable, Dict, Optional

import google.cloud.bigquery.table

from metaphor.bigquery.schema_field import SchemaFieldExtractor
from metaphor.models.metadata_change_event import (
    DatasetSchema,
    MaterializationType,
    SchemaType,
    SQLSchema,
)


class TableExtractor:

    _table_type_to_schema: Dict[
        str,
        Callable[[google.cloud.bigquery.table.Table], SQLSchema],
    ] = {
        "TABLE": lambda _: SQLSchema(materialization=MaterializationType.TABLE),
        "EXTERNAL": lambda _: SQLSchema(materialization=MaterializationType.EXTERNAL),
        "VIEW": lambda table: SQLSchema(
            materialization=MaterializationType.VIEW, table_schema=table.view_query
        ),
        "MATERIALIZED_VIEW": lambda table: SQLSchema(
            materialization=MaterializationType.MATERIALIZED_VIEW,
            table_schema=table.mview_query,
        ),
        "SNAPSHOT": lambda table: SQLSchema(
            materialization=MaterializationType.SNAPSHOT,
            snapshot_time=TableExtractor._get_snapshot_time(table),
        ),
    }

    @staticmethod
    def extract_schema(table: google.cloud.bigquery.table.Table) -> DatasetSchema:
        """
        Extracts dataset schema from the BigQuery table.
        """
        if table.table_type not in TableExtractor._table_type_to_schema.keys():
            raise ValueError(f"Unexpected table type {table.table_type}")

        schema = DatasetSchema(
            description=typing.cast(Optional[str], table.description),
            schema_type=SchemaType.SQL,
            sql_schema=TableExtractor._table_type_to_schema[str(table.table_type)](
                table
            ),
        )
        schema.fields = SchemaFieldExtractor.parse_fields(table.schema, "")

        return schema

    @staticmethod
    def _get_snapshot_time(
        table: google.cloud.bigquery.table.Table,
    ) -> Optional[datetime]:
        # bigquery client fails to parse snapshot time sometimes
        # See https://github.com/googleapis/python-bigquery/issues/1986
        try:
            if table.snapshot_definition:
                return table.snapshot_definition.snapshot_time
        except ValueError:
            return None

        return None
