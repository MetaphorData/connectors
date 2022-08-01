# PostgreSQL Connector

This connector extracts technical metadata from a PostgreSQL database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Setup

You must run the connector using a user with `SELECT` [privilege](https://www.postgresql.org/docs/current/ddl-priv.html) to the following tables:

- `pg_catalog.pg_constraint`
- `pg_catalog.pg_class`
- `pg_catalog.pg_namespace`
- `pg_catalog.pg_attribute`
- `pg_catalog.pg_description`

Or, use the following command to grant the privileges:

```sql
GRANT SELECT ON pg_catalog.pg_constraint, pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_attribute, pg_catalog.pg_description TO [User]
```

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

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations

By default, the connector will connect using the default PostgreSQL port 5432. You can change it using the following config:

```yaml
port: <port_number>
```

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `postgresql` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor postgresql <config_file>
```

Manually verify the output after the run finishes.
