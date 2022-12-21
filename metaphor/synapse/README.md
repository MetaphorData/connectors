# Synapse Connector

This connector extracts technical metadata from Azure Synapse workspaces using [System catalog views](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/catalog-views-transact-sql?view=sql-server-ver16).

## Setup

1. Setup Synapse SQL login and choose either of the way to setup SQL username and password:
    - Set up SQL admin username and admin password from Synapse workspace in [Azure portal](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Synapse%2Fworkspaces).
    - set up non-admin username and password for Synapse connector following this [tutorial](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/sql-authentication?tabs=serverless)

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
