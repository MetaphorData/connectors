# Snowflake Connector

This connector extracts technical metadata from a Snowflake account using [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html).

## Setup

We recommend creating a dedicated Snowflake user with limited permissions for the connector to use by running the following statements using `ACCOUNTADMIN` role:

```sql
set warehouse = '<warehouse>';
set db = '<database>';

-- Create metaphor_role
create role metaphor_role comment = 'Limited access role for Metaphor connector';
grant usage on warehouse identifier($warehouse) to role metaphor_role;
grant usage on all schemas in database identifier($db) to role metaphor_role;
grant usage on future schemas in database identifier($db) to role metaphor_role;
grant references on all tables in database identifier($db) to role metaphor_role;
grant references on future tables in database identifier($db) to role metaphor_role;
grant references on all views in database identifier($db) to role metaphor_role;
grant references on future views in database identifier($db) to role metaphor_role;
grant references on all materialized views in database identifier($db) to role metaphor_role;
grant references on future materialized views in database identifier($db) to role metaphor_role;

-- Create metaphor_user
create user metaphor_user
    password = '<password>'
    default_warehouse = $warehouse
    default_role = metaphor_role
    comment = 'User for Metaphor connector';
grant role metaphor_role to user metaphor_user;
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
