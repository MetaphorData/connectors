# MSSQL Connector

This connector extracts technical metadata from a Microsoft SQL Server using [System catalog views](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/catalog-views-transact-sql?view=sql-server-ver16).

## Setup
We recommend creating a dedicated MSSQL user with limited permissions for the connector to use

Use the following command to set up the read-only user for MSSQL database

```sql
-- nagivate to master database
CREATE LOGIN <username> WITH PASSWORD = '<password>'
CREATE USER <username> FROM LOGIN <username>
-- switch to user MSSQL database
CREATE USER <username> FROM LOGIN <username>
GRANT VIEW DEFINITION TO <username>
GRANT VIEW DATABASE STATE TO <username>
```
> Note: You'll need to run the above command for each database you'd like to connect to.


### Required Configurations

```yaml
username: <username>  # The MSSQL server login username

password: <password>  # The MSSQL server login password

endpoint: <endpoint>  # The MSSQL server endpoint

server_name: <sever_name>  # (optional) specify server name for MSSQL server

tenant_id: <tenant_id>  # (optional) The azure directory (tenant) id


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
