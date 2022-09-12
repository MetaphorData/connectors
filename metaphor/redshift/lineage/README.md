# Redshift Lineage Connector

This connector extracts lineage information from a Redshift database using [asyncpg](https://github.com/MagicStack/asyncpg) library.

## Setup

Follow the [Setup](../README.md#Setup) guide for the general Redshift connector to create the dedicated `metaphor` user. As the usage connector extracts information from system tables such as `STL_SCAN` & `STL_INSERT`, it needs to be given additional permissions using the following command:

```sql
ALTER USER metaphor WITH SYSLOG ACCESS UNRESTRICTED;
```

## Config File

The config file inherits all the required and optional fields from the general Redshift connector [Config File](../README.md#config-file).

### Optional Configurations

In addition, you can specify the following configurations:

```yaml
# # (Optional) Whether to enable parsing view definition to build view lineage, default True
enable_view_lineage: <boolean>

# (Optional) Whether to enable parsing stl_scan table to find table lineage information, default True
enable_lineage_from_log: <boolean>

# (Optional) Whether to include self-referencing loops in lineage, default True
include_self_lineage: <boolean>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `redshift` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```bash
metaphor redshift.lineage <config_file>
```

Manually verify the output after the run finishes.
