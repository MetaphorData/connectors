# Synapse Connector

This connector extracts technical metadata from Azure Synapse workspaces using [System catalog views](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/catalog-views-transact-sql?view=sql-server-ver16).

## Setup

1. We recommend creating a dedicated Synapse user with limited permissions for the connector to use
    - Set up the SQL admin username and password from Synapse workspace in [Azure portal](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Synapse%2Fworkspaces).
    - You could directly use the above admin user to process the Synapse connector.
      For security and privacy reasons, we recommend creating a read-only user to process the Synapse connector.
      1. Set up the read-only user for serverless SQL databases
          ```sql
          -- nagivate to master database
          CREATE LOGIN <username> WITH PASSWORD = '<password>'
          GRANT CONNECT ANY DATABASE TO <username>;
          GRANT VIEW ANY DEFINITION TO  <username>;
          ```
      2. Set up the read-only user for dedicated SQL databases
          ```sql
          -- nagivate to master database
          CREATE LOGIN <username> WITH PASSWORD = '<password>'
          CREATE USER <username> FROM LOGIN <username>
          -- switch to user dedicated SQL database
          CREATE USER <username> FROM LOGIN <username>
          GRANT VIEW DEFINITION TO <username>
          GRANT VIEW DATABASE STATE TO <username>
          ```
          > Note: You'll need to run the above command for each database you'd like to connect to.

2. (Optional) Enable the query log by setting `lookback_days` in the config file

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
tenant_id: <tenant_id>  # The azure directory (tenant) id

server_name: <workspace_name> # The Microsoft Synapse workspace name

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
