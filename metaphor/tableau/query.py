from typing import Dict

from metaphor.models.metadata_change_event import DataPlatform

connection_type_map: Dict[str, DataPlatform] = {
    "bigquery": DataPlatform.BIGQUERY,
    "postgres": DataPlatform.POSTGRESQL,
    "redshift": DataPlatform.REDSHIFT,
    "snowflake": DataPlatform.SNOWFLAKE,
}

# NOTE!!! Tableau models data source as a downstream of database, NOT upstream.
workbooks_graphql_query = """
query {
  workbooks {
    luid
    name
    projectName
    vizportalUrlId
    upstreamTables {
      name
      fullName
      schema
      database {
        name
        connectionType
      }
    }
  }
}
"""
