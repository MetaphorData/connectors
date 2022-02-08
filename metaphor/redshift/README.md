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
```

> Note: If the Redshift cluster contains more than one database, you must grant the permission in all databases. Alternatively, you can limit the connector to a subset of databases using the [filter configuration](../common/docs/filter.md).

## Config File

Create a YAML config file based on the following template.

### Required Configurations

If using user password authentication:

```yaml
host: <databse_hostname>
user: <username>
password: <password>
database: <default_database_for_connections>
output:
  file:
    directory: <output_directory>
```

See [Common Configurations](../common/README.md) for more information on `output`.

### Optional Configurations

By default, the connector will connect using the default Redshift port 5439. You can change it using the following config:

```yaml
port: <port_number>
```

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `redshift` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
python -m metaphor.redshift <config_file>
```

Manually verify the output after the run finishes.
