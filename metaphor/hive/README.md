# Hive Connector

This connector extracts technical metadata from Apache Hive.

## Setup

Please ensure the user running the connector has `SELECT` privilege with `WITH GRANT OPTION` specified, so that the connector can read table informations from Hive. See [SQL Standard Based Hive Authorization](https://cwiki.apache.org/confluence/display/Hive/SQL+Standard+Based+Hive+Authorization) for more information.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
host: <host>
port: <port>
auth_user: <auth user for hiveserver>
password: <password for the auth user>
```

For testing environments there could be no authentication. In that case, do not set `auth_user` and `password`

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Column Statistics

See [Column Statistics](../../common/docs/column_statistics.md) for details.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

Run the following command to test the connector locally:

```shell
metaphor hive <config_file>
```

Manually verify the output after the command finishes.

### Setup Apache Hive Instance for Testing

To locally setup an Apache Hive service, run the following command:

```shell
docker-compose -f metaphor/hive/docker-compose.yml up -d
```

This sets up an Apache Hive service, along with several pre-populated table definitions.
