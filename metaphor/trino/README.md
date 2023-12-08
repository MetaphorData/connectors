# Trino Connector

This connector extracts metadata from Trino using [Trino Python Client](https://github.com/trinodb/trino-python-client).

## Setup

To run a Trino db instance locally, create the docker container with:
```shell
docker run --name trino -d -p 8080:8080 trinodb/trino
```

It comes with pre-populated data by default. To populate it with your own data, see [this guide](https://trino.io/docs/current/installation/containers.html#configuring-trino).

## Config File

Create a YAML config file based on the following template.

### Required Configurations

You must specify the host, port, and the user in order to connect to Trino.
```yaml
host: <host>
port: <port>
username: <username>
output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations

#### Authentication

Currently password and JWT token based authentication methods are supported.

```yaml
password: <password> # For basic authentication
token: <token> # For JWT token
```

`password` takes precendence over `token`. For locally running Trino instances for testing purposes, leave these 2 configuration values as unset to bypass authentication altogether.

See [Trino doc](https://trino.io/docs/current/security/authentication-types.html) for more information on authentication methods.

#### HTTP scheme

If your Trino instance has TLS enabled, set `http_scheme` in the configuration file:

```yaml
http_scheme: <http_scheme>
```

Either leave as blank for HTTP, or specify `https`. Other values are not permitted.

See [Trino doc](https://trino.io/docs/current/security/tls.html) for more information.

#### Filtering

You can filter the topics you want to include in the ingested result:

```yaml
filter:
  includes: <set of patterns to include>
  excludes: <set of patterns to exclude>
```

By default the following are excluded:

- Catalog `system`
- All schema called `information_schema`

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `trino` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor trino <config_file>
```

Manually verify the output after the run finishes.