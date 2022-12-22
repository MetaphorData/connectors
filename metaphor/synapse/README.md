# Synapse Connector

This connector extracts technical metadata from Azure Synapse workspaces using [System catalog views](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/catalog-views-transact-sql?view=sql-server-ver16).

## Setup

1. Setup Synapse SQL login and SQL user:
    - Set up SQL admin username and admin password from Synapse workspace in [Azure portal](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Synapse%2Fworkspaces).
    - You could directly use the above admin user to process the Synapse connector.
      For security and privacy reasons, we recommend creating a read-only user to process the Synapse connector.
      There are many ways to create read-only users. You could refer [tutorial](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/sql-authentication?tabs=provisioned#non-administrator-users)
      or follow steps:
      1. Open [Synapse studio](https://learn.microsoft.com/en-us/azure/synapse-analytics/get-started-create-workspace#open-synapse-studio):
      2. Select `Develop` on the left panel and create one SQL script

      3. If you have both serverless SQL databases and dedicated SQL databases, need to set them up respectively
          - Serverless: <br>
            Create the read-only user for all databases
            ```bash
            # nagivate to master database
            CREATE LOGIN <username> WITH PASSWORD = '<password>'
            CREATE USER <username> FROM LOGIN <username>
            GRANT CONNECT ANY DATABASE TO <username>;
            GRANT SELECT ALL USER SECURABLES TO <username>;
            ```
          - Dedicated SQL: <br>
            Create the read-only user and add the permssion for every dedicated SQL database
            ```bash
            # nagivate to master database
            CREATE LOGIN <username> WITH PASSWORD = '<password>'
            # switch to user dedicated SQL database
            CREATE USER <username> FROM LOGIN <username>
            GRANT VIEW DATABASE STATE TO <username>
            EXEC sp_addrolemember 'db_datareader', '<username>';
          ```

   [`CAUTION`] Currently only supports one user to process Synapse connector. To crawl all databases you need. set up a single read-only user for all databases you need.

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
