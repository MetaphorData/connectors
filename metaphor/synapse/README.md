# Synapse Connector

This connector extracts technical metadata from Azure Synapse workspaces using [System catalog views](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/catalog-views-transact-sql?view=sql-server-ver16).

## Setup

Create and grant read-only permissions to a dedicated user in [Synapse workspace](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Synapse%2Fworkspaces).
- For [serverless pool](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/on-demand-workspace-overview):
    ```sql
    USE master;
    CREATE LOGIN <username> WITH PASSWORD = <password>;
    GRANT CONNECT ANY DATABASE TO <username>;
    GRANT VIEW ANY DEFINITION TO  <username>;
    ```

- For [dedicated pool](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-overview-what-is):
    ```sql
    USE master;
    CREATE LOGIN <username> WITH PASSWORD = <password>;
    CREATE USER <username> FROM LOGIN <username>;
    
    -- For each database of interest
    USE <database>;
    CREATE USER <username> FROM LOGIN <username>;
    GRANT VIEW DEFINITION TO <username>;
    GRANT VIEW DATABASE STATE TO <username>;
    ```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
tenant_id: <tenant_id>  # Azure tenant ID
server_name: <workspace_name>  # Synapse workspace name
username: <username>
password: <password>
```

### Optional Configurations

By default, the connector will not crawl the query log unless you specify `lookback_days`.

```yaml
query_log:
  lookback_days: <days>
```

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `synapse` extra.

Run the following command to test the connector locally:

```shell
metaphor synapse <config_file>
```

Manually verify the output after the command finishes.
