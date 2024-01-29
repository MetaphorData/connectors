# Redshift Connector

This connector extracts technical metadata from a Redshift database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Setup

The connector extracts the metadata from [system catalogs](https://docs.aws.amazon.com/redshift/latest/dg/c_intro_catalog_views.html), with restricted access to system tables and additional `SELECT` [privilege](https://www.postgresql.org/docs/current/ddl-priv.html) to `pg_catalog.svv_table_info`.  

Use the following command to grant the permission:

```sql
# Create a new user called "metaphor"
CREATE USER metaphor PASSWORD <password>;

# Grant minimally required privleages to the user
GRANT SELECT ON pg_catalog.svv_table_info TO metaphor;

# Grant access to syslog "STL_SCAN" and "STL_QUERY"
ALTER USER metaphor WITH SYSLOG ACCESS UNRESTRICTED;
```

> Note: If the Redshift cluster contains more than one database, you must grant the permission in all databases. Alternatively, you can limit the connector to a subset of databases using the [filter configuration](../common/docs/filter.md).

### Redshift Spectrum

To extract external tables' metadata from `SVV_EXTERNAL_*`, you must grant non-admin users `USAGE` privilege to the corresponding external schemas (see [this page](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_EXTERNAL_TABLES.html) for more details):

```SQL
GRANT USAGE ON SCHEMA <external_schema> TO metaphor;
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

If using user password authentication:

```yaml
host: <database_hostname>
user: <username>
password: <password>
database: <default_database_for_connections>
```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Port

By default, the connector will connect using the default Redshift port 5439. You can change it using the following config:

```yaml
port: <port_number>
```

#### Filtering

See [Filter Config](../common/docs/filter.md) for more information on the optional `filter` config.

#### Tag Assignment

See [Tag Matcher Config](../common/docs/tag_matcher.md) for more information on the optional `tag_matcher` config.

#### Query Logs

By default, the Redshift connector will fetch a full day's query logs from yesterday, to be analyzed for additional metadata, such as dataset usage and lineage information. To backfill log data, one can set `lookback_days` to the desired value. To turn off query log fetching, set `lookback_days` to 0.  

```yaml

query_log:
  # (Optional) Number of days of query logs to fetch. Default to 1. If 0, the no query logs will be fetched.
  lookback_days: <days>
    
  # (Optional) A list of users whose queries will be excluded from the log fetching.
  excluded_usernames:
    - <user_name1>
    - <user_name2>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `redshift` extra.

Run the following command to test the connector locally:

```shell
metaphor redshift <config_file>
```

Manually verify the output after the run finishes.
