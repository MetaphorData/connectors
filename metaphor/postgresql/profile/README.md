# PostgreSQL Data Profiling Connector

This connector extracts column-level data profiles from a PostgreSQL database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Setup

You must run the connector using a user with `SELECT` [privilege](https://www.postgresql.org/docs/current/ddl-priv.html) to all tables.

You can use the following command against all schemas:

```sql
GRANT SELECT ON ALL TABLES IN SCHEMA [Schema] TO [User]
```

## Config File

The config file inherits all the required and optional fields from the general PostgreSQL connector [Config File](../README.md#config-file).

### Optional Configurations

#### Sampling

See [Sampling Config](../../common/docs/sampling.md) for details.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `postgresql` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor postgresql.profile <config_file>
```

Manually verify the output after the run finishes.
