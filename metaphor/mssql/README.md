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

output:
  file:
    directory: <output_directory>  # the output result directory
```

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations

You could specfic `server_name` or `tenant_id` to connector.

```yaml
server_name: <sever_name>  # specify server name for MSSQL server

tenant_id: <tenant_id>  # The azure directory (tenant) id
```

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `mssql` extra.

To test the connector locally, change the config file to output to a local path and run the following command.

```shell
metaphor mssql <config_file>
```

Manually verify the output after the command finishes.

### Installing on Apple Silicon machines

If you encounter linking issues when installing `pymssql` on a machine with Apple Silicon CPU, here is a possible fix:
1. Set poetry config to build the package instead of using pre-compiled binary:
```shell
poetry config --local installer.no-binary pymssql
```
2. Instead of using system wide `ssl` module, install `openssl` and `FreeTDS` with Homebrew:
```shell
brew install FreeTDS openssl@1.1
```
3. Set `LDFLAGS` and `CFLAGS` when installing `pymssql`:
```shell
LDFLAGS="-L$(brew --prefix)/opt/freetds/lib -L$(brew --prefix)/opt/openssl@1.1/lib" \
CFLAGS="-I$(brew --prefix)/opt/freetds/include -I$(brew --prefix)/opt/openssl@1.1/include" \
poetry install -E mssql
```

If the above procedure does not fix the issue, please file an issue ticket.
