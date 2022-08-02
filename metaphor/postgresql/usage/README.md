# PostgreSQL Usage Statistics Connector

This connector extracts usage statistics from a PostgreSQL database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Config File

The config file inherits all the required and optional fields from the general PostgreSQL connector [Config File](../README.md#config-file).

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `postgresql` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor postgresql.usage <config_file>
```

Manually verify the output after the run finishes.