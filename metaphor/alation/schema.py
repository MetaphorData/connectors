from typing import Any, List, Optional

from pydantic import BaseModel, Field

from metaphor.models.metadata_change_event import (
    AssetDescription,
    ColumnDescriptionAssignment,
    DataPlatform,
    DescriptionAssignment,
)


class CustomField(BaseModel):
    value: Any
    field_id: int
    field_name: str


class Column(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    custom_fields: List[CustomField] = Field(default_factory=list)


class Table(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    ds_id: int
    custom_fields: List[CustomField] = Field(default_factory=list)
    table_type: str
    schema_id: int

    def description_assignment(self, author: Optional[str], columns: List[Column]):
        asset_descriptions: Optional[List[AssetDescription]] = None
        if self.description:
            asset_descriptions = [
                AssetDescription(author=author, description=self.description)
            ]

        column_description_assignments: List[ColumnDescriptionAssignment] = []
        for column in columns:
            if column.description:
                column_description_assignments.append(
                    ColumnDescriptionAssignment(
                        asset_descriptions=[
                            AssetDescription(
                                author=author, description=column.description
                            )
                        ],
                        column_name=column.name,
                    )
                )

        description_assignment: Optional[DescriptionAssignment] = None
        if asset_descriptions or column_description_assignments:
            description_assignment = DescriptionAssignment(
                asset_descriptions=asset_descriptions,
                column_description_assignments=column_description_assignments
                if column_description_assignments
                else None,
            )

        return description_assignment


class Schema(BaseModel):
    ds_id: int
    key: str
    """
    `ds_id`.`qualified_name`
    """

    @property
    def qualified_name(self) -> str:
        # Qualified name of the schema
        return self.key[len(str(self.ds_id)) + 1 :]


class Datasource(BaseModel):
    dbtype: str
    """
    Currently the certified types are mysql, oracle, postgresql, sqlserver, redshift, teradata and snowflake
    """
    dbname: Optional[str] = None

    @property
    def platform(self) -> DataPlatform:
        if self.dbtype == "mysql":
            return DataPlatform.MYSQL
        if self.dbtype == "postgresql":
            return DataPlatform.POSTGRESQL
        if self.dbtype == "sqlserver":
            return DataPlatform.MSSQL
        if self.dbtype == "redshift":
            return DataPlatform.REDSHIFT
        if self.dbtype == "snowflake":
            return DataPlatform.SNOWFLAKE
        return DataPlatform.EXTERNAL
