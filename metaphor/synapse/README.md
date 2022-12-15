# Synapse Connector

This connector extracts technical metadata from Azure Synapse workspaces using [System catalog views](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/catalog-views-transact-sql?view=sql-server-ver16).

## Setup

1. Setup Synapse SQL login and Microsoft `pymssql` library
    1. Set up SQL admin username and admin password for Synapse workspace in [Azure portal](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Synapse%2Fworkspaces).
    2. Follow the tutorial to install [pymssql](https://learn.microsoft.com/en-us/sql/connect/python/pymssql/step-1-configure-development-environment-for-pymssql-python-development?view=sql-server-ver16)
    3. (Optional) may need to install [FreeTDS](https://learn.microsoft.com/en-us/sql/connect/python/pymssql/step-1-configure-development-environment-for-pymssql-python-development?view=sql-server-ver16) if installing `pymssql` into errors.
    4. (Optional) if want to set up different username and password for Synapse connector follow this [tutorial](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/sql-authentication?tabs=serverless)
    5. (Optional) Apple Silicon users may encounter build the error when installing `pymssql` follow [apple_silicon](https://github.com/MetaphorData/connectors/blob/main/docs/apple_silicon.md)

2. (Optional) Enable the query log by setting `lookback_days` in the config file

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
tenant_id: <tenant_id>  # The azure directory (tenant) id

workspace_name: <workspace_name> # The Microsoft Synapse workspace name

username: <username> # The synapse workspace SQL username

password: <password> # The Synapse workspace SQL password

output:
  file:
    directory: <output_directory>  # the output result directory
```

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations
#### Query Log
```yaml
query_log:
  lookback_days: <days>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `synapse` extra.

To test the connector locally, change the config file to output to a local path and run the following command.

```shell
metaphor synapse <config_file>
```

Manually verify the output after the command finishes.
