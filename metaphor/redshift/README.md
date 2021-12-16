# Redshift Connector

This connector extracts technical metadata from a Redshift database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Setup

Currently you must run the connector using a user with `SELECT` [privilege](https://docs.aws.amazon.com/redshift/latest/dg/r_Privileges.html) to all tables & views.

> We're working on extracting the metadata from [system catalogs](https://docs.aws.amazon.com/redshift/latest/dg/c_intro_catalog_views.html), which greatly reduces the privileges required for the user.

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
    path: <path_to_output_file>
```

See [Common Configurations](../common/README.md) for more information on `output`.

### Optional Configurations

By default, the connector will connect using the default Redshift port 5439. You can change it using the following config:

```yaml
port: <port_number>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `redshift` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
python -m metaphor.redshift <config_file>
```

Manually verify the output after the run finishes.
