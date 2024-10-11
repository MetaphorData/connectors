# PostgreSQL Connector

This connector extracts technical metadata from a PostgreSQL database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Setup

You must run the connector using a user with `SELECT` [privilege](https://www.postgresql.org/docs/current/ddl-priv.html) to the following tables:

- `pg_catalog.pg_constraint`
- `pg_catalog.pg_class`
- `pg_catalog.pg_namespace`
- `pg_catalog.pg_attribute`
- `pg_catalog.pg_description`

Or, use the following command to grant the privileges:

```sql
GRANT SELECT ON pg_catalog.pg_constraint, pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_attribute, pg_catalog.pg_description TO [User]
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

If using user password authentication:

```yaml
host: <databse_hostname>
user: <username>
password: <password>
database: <default_database_for_connections>
```

### Optional Configurations

By default, the connector will connect using the default PostgreSQL port 5432. You can change it using the following config:

```yaml
port: <port_number>
```

### Query Log Extraction Configurations

The connector supports extracting query log history from CloudWatch for RDS PostgreSQL. Please follow [this documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_LogAccess.Concepts.PostgreSQL.Query_Logging.html) to configure RDS to log SQL statements to CloudWatch.

```yaml
query_log:
  aws:
    access_key_id: <aws_access_key_id>
    secret_access_key: <aws_secret_access_key>
    region_name: <aws_region_name>
    assume_role_arn: <aws_role_arn>  # If using IAM role
  logs_group: <aws_cloud_watch_logs_group>

  # (Optional) Number of days of query logs to fetch. Default to 1. If 0, the no query logs will be fetched.
  lookback_days: <days>

  # (Optional) Extract query log duration if postgres parameter `log_duration` is true
  log_duration_enabled: <bool>

  # (Optional) CloudWatch log filter pattern, https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html
  filter_pattern: <pattern>  # For example: -root LOG
    
  # (Optional) A list of users whose queries will be excluded from the log fetching.
  excluded_usernames:
    - <user_name1>
    - <user_name2>
```

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Process Query Config

See [Process Query](../common/docs/process_query.md) for more information on the optional `process_query_config` config.

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `postgresql` extra.

Run the following command to test the connector locally:

```shell
metaphor postgresql <config_file>
```

Manually verify the output after the run finishes.
