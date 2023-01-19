# MySQL Connector

This connector extracts technical metadata from a MySQL database using [PyMySQL](https://github.com/PyMySQL/PyMySQL) library.

## Setup

You must run the connector using a user with `SELECT` privilege to the databases

```sql
GRANT SELECT ON [databases] TO [User]
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

If using user password authentication:

```yaml
host: <databse_hostname>
user: <username>
password: <password>
database: <default_database_for_connections>
output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations

By default, the connector will connect using the default MySQL port 3306. You can change it using the following config:

```yaml
port: <port_number>
```

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `mysql` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor mysql <config_file>
```

Manually verify the output after the run finishes.
