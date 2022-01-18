# Redshift Usage Statistics Connector

This connector extracts Redshift usage statistics from a Redshift database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Setup

Follow the [Setup](../README.md#Setup) guide for the general Redshift connector.

## Config File

The config file inherits all the required and optional fields from the general Redshift connector [Config File](../README.md#config-file). In addition, you can specify the following configurations:

```yaml
# (Optional) Number of days to include in the usage analysis. Default to 30.
lookback_days: <days>

# (Optional) A list of users whose queries will be excluded from the usage calculation 
excluded_usernames:
  - <user_name1>
  - <user_name2>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `redshift` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```bash
python -m metaphor.redshift.usage <config_file>
```

Manually verify the output after the run finishes.
