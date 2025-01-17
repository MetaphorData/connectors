from typing import Optional

from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.models.metadata_change_event import DataPlatform
from metaphor.quick_sight.models import DataSource, TypeDataSourceParameters

"""
See https://docs.aws.amazon.com/quicksight/latest/APIReference/API_DataSource.html
"""
DATA_SOURCE_PLATFORM_MAP = {
    "ATHENA": DataPlatform.ATHENA,
    "AURORA": DataPlatform.MYSQL,
    "AURORA_POSTGRESQL": DataPlatform.POSTGRESQL,
    "BIGQUERY": DataPlatform.BIGQUERY,
    "DATABRICKS": DataPlatform.UNITY_CATALOG,
    "MARIADB": DataPlatform.MYSQL,
    "MYSQL": DataPlatform.MYSQL,
    "PRESTO": DataPlatform.TRINO,
    "POSTGRESQL": DataPlatform.POSTGRESQL,
    "REDSHIFT": DataPlatform.REDSHIFT,
    "ORACLE": DataPlatform.ORACLE,
    "S3": DataPlatform.S3,
    "SNOWFLAKE": DataPlatform.SNOWFLAKE,
    "SQLSERVER": DataPlatform.MSSQL,
    "TRINO": DataPlatform.TRINO,
}


def _get_database_from_parameters(parameters: TypeDataSourceParameters):
    if parameters.AuroraParameters:
        return parameters.AuroraParameters.Database

    if parameters.AuroraPostgreSqlParameters:
        return parameters.AuroraPostgreSqlParameters.Database

    if parameters.BigQueryParameters:
        return parameters.BigQueryParameters.ProjectId

    if parameters.MariaDbParameters:
        return parameters.MariaDbParameters.Database

    if parameters.MySqlParameters:
        return parameters.MySqlParameters.Database

    if parameters.OracleParameters:
        return parameters.OracleParameters.Database

    if parameters.PostgreSqlParameters:
        return parameters.PostgreSqlParameters.Database

    if parameters.RdsParameters:
        return parameters.RdsParameters.Database

    if parameters.RedshiftParameters:
        return parameters.RedshiftParameters.Database

    if parameters.SnowflakeParameters:
        return parameters.SnowflakeParameters.Database

    if parameters.SqlServerParameters:
        return parameters.SqlServerParameters.Database

    return None


def get_database(data_source: DataSource) -> Optional[str]:
    """
    Extract database from DataSource parameters
    """
    if data_source.DataSourceParameters is None:
        return None

    parameters = data_source.DataSourceParameters

    return _get_database_from_parameters(parameters)


def get_account(data_source: DataSource) -> Optional[str]:
    """
    Extract account from DataSource parameters
    """
    if data_source.DataSourceParameters is None:
        return None

    parameters = data_source.DataSourceParameters

    if parameters.AuroraParameters:
        return parameters.AuroraParameters.Host

    if parameters.MariaDbParameters:
        return parameters.MariaDbParameters.Host

    if parameters.MySqlParameters:
        return parameters.MySqlParameters.Host

    if parameters.OracleParameters:
        return parameters.OracleParameters.Host

    if parameters.SnowflakeParameters:
        return (
            normalize_snowflake_account(parameters.SnowflakeParameters.Host, True)
            if parameters.SnowflakeParameters.Host
            else None
        )

    if parameters.SqlServerParameters:
        return parameters.SqlServerParameters.Host

    return None


def get_id_from_arn(arn: str) -> str:
    """
    Extract id from arn (e.g. arn:aws:quicksight:us-west-2:1231048943:[dataset/dashboard/datasource]/xxx)
    """
    return arn.split("/")[-1]
