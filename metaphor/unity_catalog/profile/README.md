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
```

### Optional Configurations

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `unity_catalog` extra.

Run the following command to test the connector locally:

```shell
metaphor unity_catalog.profile <config_file>
```

Manually verify the output after the command finishes.
