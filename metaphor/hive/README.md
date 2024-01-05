# Hive Connector

This connector extracts technical metadata from Apache Hive.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
host: <host>
port: <port>
auth_user: <auth user for hiveserver>
password: <password for the auth user>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

To test the connector locally, change the config file to output to a local path and run the following command

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
