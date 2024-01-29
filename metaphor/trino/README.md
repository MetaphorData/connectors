# Trino Connector

This connector extracts metadata from Trino using [Trino Python Client](https://github.com/trinodb/trino-python-client).

## Setup

To run a Trino db instance locally, create the docker container with:

```shell
docker run --name trino -d -p 8080:8080 trinodb/trino
```

It comes with pre-populated data by default. To populate it with your own data, see [this guide](https://trino.io/docs/current/installation/containers.html#configuring-trino).

You must run the connector using a user with `SELECT` privilege to the catalogs:

```sql
GRANT SELECT ON <catalogs> TO <user>
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

You must specify the host, port, and the user in order to connect to Trino.

```yaml
host: <host>
port: <port>
username: <username>
```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Authentication

Currently password and JWT token based authentication methods are supported.

```yaml
password: <password> # For basic authentication
token: <token> # For JWT token
```

`password` takes precendence over `token`. For locally running Trino instances for testing purposes, leave these 2 configuration values as unset to bypass authentication altogether.

See [Trino doc](https://trino.io/docs/current/security/authentication-types.html) for more information on authentication methods.

#### Enable TLS

If your Trino instance has TLS enabled, set `enable_tls` in the configuration file:

```yaml
enable_tls: <enable_tls>
```

By default this value is set to `False`.

See [Trino doc](https://trino.io/docs/current/security/tls.html) for more information.

#### Filtering

See [Filter Config](../common/docs/filter.md) for more information on the optional `filter` config.

By default the following are excluded:

- Catalog `system`
- All schema called `information_schema`

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `trino` extra.

Run the following command to test the connector locally:

```shell
metaphor trino <config_file>
```

Manually verify the output after the run finishes.