# Tableau Connector

This connector extracts technical metadata from a Monte Carlo site using [Monte Carlo API](https://docs.getmontecarlo.com/docs/using-the-api).

## Setup

We recommend creating a dedicated API Key for the connector to use. Follow the [instruction](https://docs.getmontecarlo.com/docs/creating-an-api-token#creating-an-api-key) to create a new API key and note down the API key ID and token.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
api_key_id: <api_key_id>
api_key_secret: <api_key_token>
data_platform: <data_platform>  // SNOWFLAKE, BIG_QUERY, etc
output:
  file:
    directory: <output_directory>
```

### Optional Configurations

#### Snowflake Account

If one of the data sources is using Snowflake dataset, please provide the Snowflake account as follows,

```yaml
snowflake_account: <account_name>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `monte_carlo` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor monte_carlo <config_file>
```

Manually verify the output after the command finishes.
