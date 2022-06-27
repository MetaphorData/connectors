# Redshift Query Connector

This connector extracts query history from a Redshift database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Setup

Follow the [Setup](../README.md#Setup) guide for the general Redshift connector to create the dedicated `metaphor` user. As the query connector extracts information from system tables such as `STL_SCAN` & `STL_QUERY`, it needs to be given additional permissions using the following commands:

```sql
ALTER USER metaphor WITH SYSLOG ACCESS UNRESTRICTED;

GRANT SELECT ON pg_catalog.svl_user_info TO metaphor;
```

## Config File

The config file inherits all the required and optional fields from the general Redshift connector [Config File](../README.md#config-file). In addition, you can specify the following configurations:

```yaml
# (Optional) Number of days to include in the usage analysis. Default to 30.
lookback_days: <days>

# (Optional) Maximum number of recent queries to extract per table. Default to 100.
max_queries_per_table: <number>

# (Optional) A list of users whose queries will be excluded from the usage calculation 
excluded_usernames:
  - <user_name1>
  - <user_name2>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `redshift` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```bash
python -m metaphor.redshift.query <config_file>
```

Manually verify the output after the run finishes.
