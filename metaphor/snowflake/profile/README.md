# Snowflake Usage Statistics Connector

This connector extracts column-level data profiles from a Snowflake account using [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html).

## Setup

Create a dedicated user & role based on the [Setup](../README.md#Setup) guide for the general Snowflake connector. You'll need to grant additional permission to the role in order to access the Account Usage tables:

```sql
grant select on all tables in database identifier($db) to role metaphor_role;
grant select on future tables in database identifier($db) to role metaphor_role;
grant select on all views in database identifier($db) to role metaphor_role;
grant select on future views in database identifier($db) to role metaphor_role;
grant select on all materialized views in database identifier($db) to role metaphor_role;
grant select on future materialized views in database identifier($db) to role metaphor_role;
```

## Config File

Create a YAML config file based the following template.

```yaml
account: <snowflake_account>
user: <snowflake_username>
password: <snowflake_password>
default_database: <default_database_for_connections>
output:
  file:
    path: <path_to_output_file>
```

By default the connector will extract metadata from all databases. You can optionally limit it to specific databases by adding the `target_databases` option to the config, e.g.

```yaml
target_databases:
  - db1
  - db2
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.snowflake.profiling <config_file>
```

Manually verify the output after the run finishes.
