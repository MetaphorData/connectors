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

For tables with large number of rows, it can take a long time to profile. To speed up the process, we can choose to do percentage-based random sampling on the data by setting `sampling_percentage`:

```yaml
sampling_percentage: <number between 0 and 100>
```

For example, `sampling_percentage = 1` means random sampling of 1% rows in the table. Keep in mind that sampling won't apply to smaller tables (with less than 100K rows), in that case, we do complete table profiling.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.snowflake.profile <config_file>
```

Manually verify the output after the run finishes.
