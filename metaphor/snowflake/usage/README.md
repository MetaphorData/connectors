# Snowflake Usage Statistics Connector

This connector extracts usage statistics from a Snowflake account using [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html). It queries [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html) and therefore requires Snowflake Enterprise or higher.

## Setup

Create a dedicated user & role based on the [Setup](../README.md#Setup) guide for the general Snowflake connector.

## Config File

The config file inherits all the required and optional fields from the general Snowflake connector [Config File](../README.md#config-file). In addition, you can specify the following configurations:

```yaml
# (Optional) whether to do day-by-day log parsing and keep the metadata history, or fetch log for <lookback_days> and not keep history, default to True. 
use_history: bool = True

# (Optional) Number of days to include in the usage analysis. Default to 30.
lookback_days: <days>

# (Optional) A list of users whose queries will be excluded from the usage calculation 
excluded_usernames:
  - <user_name1>
  - <user_name2>

# (Optional) The number of access logs fetched in a batch, default to 100000 
batch_size: <batch_size>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.snowflake.usage <config_file>
```

Manually verify the output after the run finishes.
