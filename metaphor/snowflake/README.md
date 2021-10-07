# Snowflake Connector

This connector extracts technical metadata from a Snowflake account using [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html).

## Setup

We recommend creating a separate Snowflake user with limited permissions for the connector to use. You can use the following statements to create a role and grant it to the user.

```sql
create role metaphor_connector comment = 'Limited access role for Metaphor connector';

grant usage on database <database> to role metaphor_connector;
grant usage on schema <database>.information_schema to role metaphor_connector;
grant select on all tables in schema <database>.information_schema to role metaphor_connector;
grant usage on warehouse <warehouse> to role metaphor_connector;

-- TODO: permission for get_ddl: https://docs.snowflake.com/en/sql-reference/functions/get_ddl.html#usage-notes

alter user <username> set disabled = true;
grant role metaphor_connector to user <username>;
alter user <username> set default_role = metaphor_connector;
alter user <username> set disabled = false;
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
python -m metaphor.snowflake <config_file>
```

Manually verify the output after the run finishes.
