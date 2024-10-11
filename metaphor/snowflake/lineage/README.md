# (DEPRECATED) Snowflake Lineage Connector

The base Snowflake crawler already covers the query logs, and our main app will extract TLL and CLL from the collected query logs. This makes the Snowflake lineage connector redundant.

## Config File

The config file inherits all the required and optional fields from the general Snowflake connector [Config File](../README.md#config-file). In addition, you can specify the following configurations:

```yaml
# (Optional) Whether to enable finding view lineage from object dependencies, default True.
enable_view_lineage: bool = True

# (Optional) Whether to enable finding table lineage information from access history and query history, default True.
enable_lineage_from_history: bool = True

# (Optional) Whether to include self-referencing loops in lineage, default False (exclude self loop)
include_self_lineage: <boolean>

# (Optional) Number of days to include in the usage analysis. Default to 7.
lookback_days: <days>

# (Optional) The number of access logs fetched in a batch, default to 100000 
batch_size: <batch_size>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

Run the following command to test the connector locally:

```shell
metaphor snowflake.linage <config_file>
```

Manually verify the output after the run finishes.
