# Unity Catalog Connector

This connector extracts technical metadata from Unity Catalog using the [Unity Catalog API](https://api-docs.databricks.com/rest/latest/unity-catalog-api-specification-2-1.html).

## Setup

Create an access token in the Databrick workspace > `User setting` > `Access tokens`.

Data lineage should be anbled by default for all workspaces that reference Unity Catalog. You can manually enable it by following [these instructions](https://docs.databricks.com/data-governance/unity-catalog/enable-workspaces.html)

> Refer to [this document](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#limitations) to understand the limitations of Unity Catalog's data lineage.

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

#### Filtering

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Source URL

By default, each table is associated with a Unity Catalog URL derived from the `host` config.
You can override this by specifying your own URL built from the catalog, schema, and table names:

```yaml
source_url: https://example.com/view/{catalog}/{schema}/{table}
```

#### Query Logs

By default, the Unity Catalog connector will fetch a full day's query logs from yesterday, to be analyzed for additional metadata, such as dataset usage and lineage information. To backfill log data, one can set `lookback_days` to the desired value. To turn off query log fetching, set `lookback_days` to 0.  

```yaml

query_log:
  # (Optional) Number of days of query logs to fetch. Default to 1. If 0, the no query logs will be fetched.
  lookback_days: <days>
    
  # (Optional) A list of users whose queries will be excluded from the log fetching.
  excluded_usernames:
    - <user_name1>
    - <user_name2>

  # (Optional) Limit the number of results returned in one page of query log history. The default is 100.
  max_results: <count>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `unity_catalog` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor unity_catalog <config_file>
```

Manually verify the output after the command finishes.
