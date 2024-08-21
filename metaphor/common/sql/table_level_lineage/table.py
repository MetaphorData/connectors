from typing import Optional

from pydantic.dataclasses import dataclass
from sqlglot import exp

from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.models.metadata_change_event import DataPlatform, QueriedDataset


@dataclass(frozen=True)
class Table:
    db: Optional[str]
    schema: Optional[str]
    table: str

    @classmethod
    def from_sqlglot_table(cls, table: exp.Table):
        return cls(
            db=table.catalog if table.catalog else None,
            schema=table.db if table.db else None,
            table=table.name,
        )

    def to_queried_dataset(
        self,
        platform: DataPlatform,
        account: Optional[str],
        default_database: Optional[str] = None,
        default_schema: Optional[str] = None,
    ):
        schema = self.schema or default_schema
        database = self.db or default_database
        return QueriedDataset(
            database=database,
            schema=schema,
            table=self.table,
            id=str(
                to_dataset_entity_id(
                    dataset_normalized_name(database, schema, self.table),
                    platform,
                    account,
                )
            ),
        )
