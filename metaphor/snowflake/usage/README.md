# Snowflake Usage Statistics Connector

This connector extracts usage statistics from a Snowflake account using [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html).

## Setup

Create a dedicated user & role based on the [Setup](../README.md#Setup) guide for the general Snowflake connector. You'll need to grant additional permission to the role in order to access the [Account Usage](https://docs.snowflake.com/en/sql-reference/account-usage.html#enabling-account-usage-for-other-roles) tables:

```sql
grant imported privileges on database snowflake to role metaphor_role;
```

## Config File

Create a YAML config file based on the following template.

### Required configurations

Follow the same [required configurations instruction](../README.md#required-configurations).

### Optional configurations

By default, the connector will extract usage statistics from all tables in all databases for all users. You can limit the scope by specifying the following optional configs:

```yaml
# Databases to be included
included_databases:
  - db1
  - db2

# Tables to be included
included_table_names:
  - table1
  - table2

# Tables to be excluded
excluded_table_names:
  - table3
  - table4

# Exclude queries issued by specific users from the usage statistics
excluded_usernames:
  - user1
  - user2
```

The max number of concurrent queries to the snowflake database can be configured as follows, the default is 10.

```yaml
max_concurrency: <max_number_of_queries>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.snowflake.usage <config_file>
```

Manually verify the output after the run finishes.
