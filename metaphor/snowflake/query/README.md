# Snowflake Query Connector

This connector extracts query history from a Snowflake account using [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html). It queries [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html) and therefore requires Snowflake Enterprise or higher.

## Setup

Create a dedicated user & role based on the [Setup](../README.md#Setup) guide for the general Snowflake connector.

## Config File

The config file inherits all the required and optional fields from the general Snowflake connector [Config File](../README.md#config-file). In addition, you can specify the following configurations:

```yaml
# (Optional) Number of days of logs to extract for lineage analysis. Default to 30.
lookback_days: <days>

# (Optional) The number of access logs fetched in a batch, default to 100000, value must be in range 0 - 100000
batch_size: <batch_size>

# (Optional) Maximum number of recent queries to extract per table. Default to 100.
max_queries_per_table: <number>

# (Optional) Exclude queries issued by these users.
excluded_usernames:
  - user1
  - user2
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.snowflake.query <config_file>
```

Manually verify the output after the run finishes.
