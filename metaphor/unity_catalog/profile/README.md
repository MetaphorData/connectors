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
hostname: <cluster_or_warehouse_hostname>
http_path: <http_path>
token: <access_token>
```

See [this page](https://docs.databricks.com/en/integrations/compute-details.html) for details on how to set the values for `hostname` and `http_path`.

### Optional Configurations

See [Filter Configurations](../../common/docs/filter.md) for more information on the optional `filter` config.

#### Output Destination

See [Output Config](../../common/docs/output.md) for more information on the optional `output` config.

#### Concurrency

The max number of concurrent queries to the databricks compute node can be configured as follows,

```yaml
max_concurrency: <max_number_of_queries> # Default to 10
```

#### Analyze table

To run `ANALYZE TABLE` query if there are not statistics for the table.

```yaml
analyze_if_no_statistics: true # Default is false
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `unity_catalog` extra.

Run the following command to test the connector locally:

```shell
metaphor unity_catalog.profile <config_file>
```

Manually verify the output after the command finishes.
