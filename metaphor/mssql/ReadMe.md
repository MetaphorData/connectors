# MsSQL Connector

This connector extracts technical metadata from a Microsoft SQL Server using [System catalog views](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/catalog-views-transact-sql?view=sql-server-ver16).

## Setup
We recommend creating a dedicated MsSQL user with limited permissions for the connector to use

Use the following command to set up the read-only user for MsSQL database

```sql
-- nagivate to master database
CREATE LOGIN <username> WITH PASSWORD = '<password>'
CREATE USER <username> FROM LOGIN <username>
-- switch to user MsSQL database
CREATE USER <username> FROM LOGIN <username>
GRANT VIEW DEFINITION TO <username>
GRANT VIEW DATABASE STATE TO <username>
```
> Note: You'll need to run the above command for each database you'd like to connect to.


### Required Configurations

```yaml
tenant_id: <tenant_id>  # The azure directory (tenant) id

server_name: <workspace_name>  # The MsSQL server name

username: <username>  # The MsSQL server login username

password: <password>  # The MsSQL server login password

output:
  file:
    directory: <output_directory>  # the output result directory
```

See [Output Config](../common/docs/output.md) for more information on `output`.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `mssql` extra.

To test the connector locally, change the config file to output to a local path and run the following command.

```shell
metaphor mssql <config_file>
```

Manually verify the output after the command finishes.