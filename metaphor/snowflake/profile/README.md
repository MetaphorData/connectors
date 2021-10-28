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

Create a YAML config file based on the following template.

### Required configurations

Follow the same [required configurations instruction](../README.md#required-configurations).

### Optional configurations

By default, the connector will extract metadata from all databases. You can optionally limit it to specific databases by adding the `target_databases` option to the config, e.g.

```yaml
target_databases:
  - db1
  - db2
```

The max number of concurrent queries to the snowflake database can be configured as follows, the default is 10.

```yaml
max_concurrency: <max_number_of_queries>
```

By default, the connector only profile Snowflake "base table", i.e. exclude views and temporary tables. The following config can enable profiling on all tables and views.

```yaml
exclude_views: false
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.snowflake.profile <config_file>
```

Manually verify the output after the run finishes.
