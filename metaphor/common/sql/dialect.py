from metaphor.models.metadata_change_event import DataPlatform

PLATFORM_TO_DIALECT = {
    DataPlatform.BIGQUERY: "bigquery",
    DataPlatform.HIVE: "hive",
    DataPlatform.MSSQL: "tsql",
    DataPlatform.MYSQL: "mysql",
    DataPlatform.POSTGRESQL: "postgres",
    DataPlatform.REDSHIFT: "redshift",
    DataPlatform.SNOWFLAKE: "snowflake",
    DataPlatform.UNITY_CATALOG: "databricks",
    DataPlatform.ORACLE: "oracle",
}
"""
Mapping from data platforms supported by Metaphor to dialects recognized by SQLGlot.
"""
