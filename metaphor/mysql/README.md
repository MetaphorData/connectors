# MySQL Connector

This connector extracts technical metadata from a MySQL database using [PyMySQL](https://github.com/PyMySQL/PyMySQL) library.

## Setup

You must run the connector using a user with `SELECT` privilege to the databases

```sql
GRANT SELECT ON <databases> TO <user>
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
```

### Optional Configurations

By default, the connector will connect using the default MySQL port 3306. You can change it using the following config:

```yaml
port: <port_number>
```

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `mysql` extra.

Run the following command to test the connector locally:

```shell
metaphor mysql <config_file>
```

Manually verify the output after the run finishes.
