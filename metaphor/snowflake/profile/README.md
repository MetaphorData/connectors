# Snowflake Data Profiling Connector

This connector extracts column-level data profiles from a Snowflake account using [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html).

## Setup

Create a dedicated user & role based on the [Setup](../README.md#Setup) guide for the general Snowflake connector. You'll need to grant additional permissions to the role in order to execute `SELECT` statements against all tables:

```sql
grant select on all tables in database identifier($db) to role metaphor_role;
grant select on future tables in database identifier($db) to role metaphor_role;
grant select on all views in database identifier($db) to role metaphor_role;
grant select on future views in database identifier($db) to role metaphor_role;
grant select on all materialized views in database identifier($db) to role metaphor_role;
grant select on future materialized views in database identifier($db) to role metaphor_role;
```

## Config File

The config file inherits all the required and optional fields from the general Snowflake connector [Config File](../README.md#config-file).

### Optional Configurations

#### Exclude Views

By default, the connector only profile Snowflake "base table", i.e. exclude views and temporary tables. The following config can enable profiling on all tables and views.

```yaml
exclude_views: false
```

#### Sampling

See [Sampling Config](../../common/docs/sampling.md) for details.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.snowflake.profile <config_file>
```

Manually verify the output after the run finishes.
