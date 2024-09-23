import metaphor.quick_sight.models as models
from metaphor.quick_sight.data_source_utils import get_account, get_database


def test_utils():
    aurora_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            AuroraParameters=models.TypeAuroraParameters(
                Database="db",
                Host="host",
                Port=123,
            )
        )
    )

    aurora_postgres_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            AuroraPostgreSqlParameters=models.TypeAuroraPostgreSqlParameters(
                Database="db",
                Host="host",
                Port=123,
            )
        )
    )

    bigquery_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            BigQueryParameters=models.TypeBigQueryParameters(ProjectId="project")
        )
    )

    maria_db_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            MariaDbParameters=models.TypeMariaDbParameters(
                Database="db",
                Host="host",
                Port=123,
            )
        )
    )

    mysql_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            MySqlParameters=models.TypeMySqlParameters(
                Database="db",
                Host="host",
                Port=123,
            )
        )
    )

    oracle_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            OracleParameters=models.TypeOracleParameters(
                Database="db",
                Host="host",
                Port=123,
            )
        )
    )

    postgres_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            PostgreSqlParameters=models.TypePostgreSqlParameters(
                Database="db",
                Host="host",
                Port=123,
            )
        )
    )

    rds_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            RdsParameters=models.TypeRdsParameters(
                Database="db",
                InstanceId="123123801",
            )
        )
    )

    redshift_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            RedshiftParameters=models.TypeRedshiftParameters(
                Database="db", Host="host", Port=123
            )
        )
    )

    snowflake_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            SnowflakeParameters=models.TypeSnowflakeParameters(
                Database="db", Host="account.snowflakecomputing.com"
            )
        )
    )

    sql_server_source = models.DataSource(
        DataSourceParameters=models.TypeDataSourceParameters(
            SqlServerParameters=models.TypeSqlServerParameters(
                Database="db", Host="host", Port=123
            )
        )
    )

    assert get_database(models.DataSource()) is None
    assert get_database(aurora_source) == "db"
    assert get_database(aurora_postgres_source) == "db"
    assert get_database(bigquery_source) == "project"
    assert get_database(maria_db_source) == "db"
    assert get_database(mysql_source) == "db"
    assert get_database(oracle_source) == "db"
    assert get_database(postgres_source) == "db"
    assert get_database(rds_source) == "db"
    assert get_database(redshift_source) == "db"
    assert get_database(snowflake_source) == "db"
    assert get_database(sql_server_source) == "db"

    assert get_account(models.DataSource()) is None
    assert get_account(aurora_source) == "host"
    assert get_account(aurora_postgres_source) is None
    assert get_account(bigquery_source) is None
    assert get_account(maria_db_source) == "host"
    assert get_account(mysql_source) == "host"
    assert get_account(oracle_source) == "host"
    assert get_account(postgres_source) is None
    assert get_account(rds_source) is None
    assert get_account(redshift_source) is None
    assert get_account(snowflake_source) == "account"
    assert get_account(sql_server_source) == "host"
