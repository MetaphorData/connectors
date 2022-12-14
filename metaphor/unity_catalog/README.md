# Unity Catalog Connector

This connector extracts technical metadata from Unity Catalog using the [Unity Catalog API](https://api-docs.databricks.com/rest/latest/unity-catalog-api-specification-2-1.html).

## Setup

Create an access token in the Databrick workspace > `User setting` > `Access tokens`.

Data lineage should be anbled by default for all workspaces that reference Unity Catalog. You can manually enable it by following [these instructions](https://docs.databricks.com/data-governance/unity-catalog/enable-workspaces.html)

> Rerfer to [this document](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#limitations) to understand the limtations of Unity Catalog's data lineage.

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

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `unity_catalog` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor unity_catalog <config_file>
```

Manually verify the output after the command finishes.
