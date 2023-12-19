# DataHub Connector

This connector extracts metadata for user generated from DataHub.

## Setup

1. Install datahub.
```shell
pip install acryl-datahub
```
2. Start quickstart datahub instance with the provided docker compose file:
```shell
datahub docker quickstart -f metaphor/datahub/docker-compose-without-neo4j-m1.quickstart.yml
```
    - For other architectures, pull https://github.com/datahub-project/datahub/blob/master/docker/quickstart/docker-compose-without-neo4j.quickstart.yml, and add `METADATA_SERVICE_AUTH_ENABLED=true` to `datahub-gms` and `datahub-frontend-react` containers' enviroment variables.

3. Once datahub starts, create a personal access token. See [the official documentation](https://datahubproject.io/docs/authentication/personal-access-tokens#creating-personal-access-tokens) for detailed process. This is the token our connector will use to connect to the datahub apis.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
host: <host>
port: <port>
token: <token> # This is the personal access token.
output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `datahub` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor datahub <config_file>
```

Manually verify the output after the run finishes.