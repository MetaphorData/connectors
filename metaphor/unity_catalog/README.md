# Unity Catalog Connector

This connector extracts technical metadata from Unity Catalog using the [Unity Catalog API](https://api-docs.databricks.com/rest/latest/unity-catalog-api-specification-2-1.html).

## Setup

Create an access token in the Databrick workspace > `User setting` > `Access tokens`.

## Config File

Create a YAML config file based on the following template.

### Configurations

```yaml
host: <workspace_url>
token: <access_token>
output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `unity_catalog` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor unity_catalog <config_file>
```

Manually verify the output after the command finishes.
