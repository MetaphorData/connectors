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
```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Snowflake, MSSQL and Synapse Account

If there are data sources from Snowflake, MSSQL or Synapse, please provide their accounts as follows,

```yaml
snowflake_account: <snowflake_account_name>
mssql_account: <mssql_account_name>
synapse_account: <synapse_account_name>
```

#### Description Author Email

DataHub does not keep track of the description authors. You can specify the description author email in the configuration file:

```yaml
description_author_email: <email>
```

If not provided, each dataset's first owner will be considered as the author. If no owner exists for a dataset, the placeholder email `admin@metaphor.io` will be used.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `datahub` extra.

Run the following command to test the connector locally:

```shell
metaphor datahub <config_file>
```

Manually verify the output after the run finishes.