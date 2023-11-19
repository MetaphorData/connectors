# Unity Catalog Data Profiling Connector

This connector extracts dataset-level data profiles from Unity Catalog using the [Unity Catalog API](https://api-docs.databricks.com/rest/latest/unity-catalog-api-specification-2-1.html).

## Setup

Create a dedicated access token based on the [Setup](../README.md#Setup) guide for the general Unity Catalog connector. You'll need to ensure the owner of the access token has `SELECT` privilege for the tables in order to analyze the table statistics:

```sql
GRANT SELECT ON TABLE * TO <user_role>
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
host: <workspace_url>
token: <access_token>
output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Warehouse ID

To run the queries using a specific warehouse, simply add its IDt  in the configuration file:

```yaml
warehouse_id: <warehouse_id>
```

If no warehouse id is provided, the connector automatically uses the first discovered warehouse.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `unity_catalog` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor unity_catalog.profile <config_file>
```

Manually verify the output after the command finishes.
