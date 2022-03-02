# Redshift Usage Statistics Connector

This connector extracts Redshift usage statistics from a Redshift database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Setup

Follow the [Setup](../README.md#Setup) guide for the general Redshift connector to create the dedicated `metaphor` user. As the usage connector extracts information from system tables such as `STL_SCAN` & `STL_QUERY`, it needs to be given additional permissions using the following commands:

```sql
ALTER USER metaphor WITH SYSLOG ACCESS UNRESTRICTED;

GRANT SELECT ON pg_catalog.svl_user_info TO metaphor;
```

## Config File

The config file inherits all the required and optional fields from the general Redshift connector [Config File](../README.md#config-file). In addition, you can specify the following configurations:

```yaml
# (Optional) whether to do day-by-day log parsing and keep the metadata history, or fetch log for <lookback_days> and not keep history, default to True. 
use_history: bool = True

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
