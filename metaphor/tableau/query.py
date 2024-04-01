from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from metaphor.models.metadata_change_event import DataPlatform

connection_type_map: Dict[str, DataPlatform] = {
    "bigquery": DataPlatform.BIGQUERY,
    "postgres": DataPlatform.POSTGRESQL,
    "redshift": DataPlatform.REDSHIFT,
    "snowflake": DataPlatform.SNOWFLAKE,
}

# NOTE!!! the id (uuid) of an entity from graphql api is different from
# the id of the same entity from the REST api, use luid instead
workbooks_graphql_query = """
query($first: Int, $offset: Int) {
  workbooksConnection(first: $first, offset: $offset) {
    nodes {
      luid
      name
      projectName
      projectVizportalUrlId
      vizportalUrlId
      upstreamDatasources {
        id
        luid
        vizportalUrlId
        name
        description
        fields {
          name
          description
        }
        upstreamTables {
          luid
          name
          fullName
          schema
          database {
            name
            connectionType
          }
        }
        owner {
          luid
        }
      }
      embeddedDatasources {
        id
        name
        fields {
          name
          description
        }
        upstreamTables {
          luid
          name
          fullName
          schema
          database {
            name
            connectionType
          }
        }
      }
      tags {
        name
      }
    }
  }
}
"""


class Database(BaseModel):
    name: str
    description: Optional[str] = None
    connectionType: str


class DatabaseTable(BaseModel):
    luid: str
    name: str
    fullName: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    description: Optional[str] = None
    database: Optional[Database] = None


class DatasourceField(BaseModel):
    name: str
    description: Optional[str] = None


class Owner(BaseModel):
    luid: str


class PublishedDatasource(BaseModel):
    id: str
    luid: str
    vizportalUrlId: str
    name: str
    description: Optional[str] = None
    fields: List[DatasourceField]
    upstreamTables: List[DatabaseTable]
    owner: Owner


class EmbeddedDatasource(BaseModel):
    id: str
    name: str
    fields: List[DatasourceField]
    upstreamTables: List[DatabaseTable]


class Tag(BaseModel):
    name: str


class WorkbookQueryResponse(BaseModel):
    """Modeling Metadata Graphql API response for a workbook"""

    luid: str
    name: str
    projectName: str
    projectVizportalUrlId: str
    vizportalUrlId: str
    upstreamDatasources: List[PublishedDatasource]
    embeddedDatasources: List[EmbeddedDatasource]
    tags: List[Tag]


# GraphQL that lists all custom SQL queries used in datasources.
# To prevent the server from returning partial results when the query takes too long to run, we
# 1. Run this as a separate query from the workbooks GraphQL
# 2. Only return the first column as the datasource ID is the same for every column
custom_sql_graphql_query = """
query($first: Int, $offset: Int) {
  customSQLTablesConnection(first: $first, offset: $offset) {
    nodes {
      id
      query
      connectionType
      columnsConnection(first: 1, offset: 0) {
        nodes {
          ...on Column {
            referencedByFields {
              datasource {
                id
              }
            }
          }
        }
      }
    }
  }
}
"""


class DataSource(BaseModel):
    id: str


class ReferencedByField(BaseModel):
    datasource: DataSource


class TableColumn(BaseModel):
    referencedByFields: List[ReferencedByField]


class ColumnConnection(BaseModel):
    nodes: List[TableColumn]


class CustomSqlTable(BaseModel):
    """Modeling Metadata Graphql API response for a custom SQL table"""

    id: str
    query: str
    connectionType: str
    columnsConnection: ColumnConnection
