# PostgreSQL Usage Statistics Connector

This connector extracts usage statistics from a PostgreSQL database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Config File

The config file inherits all the required and optional fields from the general PostgreSQL connector [Config File](../README.md#config-file).

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `postgresql` extra.

Run the following command to test the connector locally:

```shell
metaphor postgresql.usage <config_file>
```

Manually verify the output after the run finishes.
