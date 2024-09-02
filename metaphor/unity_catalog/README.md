# Unity Catalog Connector

This connector extracts technical metadata from Unity Catalog using the [Unity Catalog API](https://api-docs.databricks.com/rest/latest/unity-catalog-api-specification-2-1.html).

## Setup

Create an access token in the Databrick workspace > `User setting` > `Developer` > `Access tokens`.

To extract [data lineage](https://docs.databricks.com/en/data-governance/unity-catalog/data-lineage.html) from Unity Catalog, you'll need to [enable system.access schema]() and [grant required permissions](https://docs.databricks.com/en/admin/system-tables/index.html#grant-access-to-system-tables) to the user. Please also read and understand the feature's [limitations](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#limitations).

Make sure to grant the user BROWSE (or SELECT) privilege to all tables in order to retrieve the complete lineage graph. See [this section](https://docs.databricks.com/en/data-governance/unity-catalog/data-lineage.html#lineage-permissions) for more details.

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

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Filtering

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Source URL

By default, each table is associated with a Unity Catalog URL derived from the `hostname` config.

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

##### Process Query Config

See [Process Query](../common/docs/process_query.md) for more information on the optional `process_query_config` config.

#### Warehouse ID

Note: we encourage using cluster, this connector will deprecate the SQL warehouse support.

To run the queries using a specific warehouse, simply add its ID in the configuration file:

```yaml
warehouse_id: <warehouse_id>
```

If no warehouse id nor cluster path is provided, the connector automatically uses the first discovered warehouse.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `unity_catalog` extra.

Run the following command to test the connector locally:

```shell
metaphor unity_catalog <config_file>
```

Manually verify the output after the command finishes.
