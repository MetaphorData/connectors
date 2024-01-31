# MSSQL Connector

This connector extracts technical metadata from a Microsoft SQL Server using [System catalog views](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/catalog-views-transact-sql?view=sql-server-ver16).
> Note: For on-permise MSSQL server, we support SQL Server 2016 (13.x) and later

## Setup

We recommend creating a dedicated MSSQL user with limited permissions for the connector to use

Use the following command to set up the read-only user for MSSQL database

```sql
-- navigate to master database
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
```

### Optional Configurations

You could specfic `server_name` or `tenant_id` to connector.

```yaml
server_name: <sever_name>  # specify server name for MSSQL server

tenant_id: <tenant_id>  # The azure directory (tenant) id
```

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `mssql` extra.

Run the following command to test the connector locally:

```shell
metaphor mssql <config_file>
```

Manually verify the output after the command finishes.
