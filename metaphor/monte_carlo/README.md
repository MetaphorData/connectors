# Monte Carlo Connector

This connector extracts technical metadata from a Monte Carlo site using [Monte Carlo API](https://docs.getmontecarlo.com/docs/using-the-api).

## Setup

We recommend creating a dedicated API Key for the connector to use. Follow [these instructions](https://docs.getmontecarlo.com/docs/creating-an-api-token#creating-an-api-key) to create a new API key and secret.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
api_key_id: <api_key_id>
api_key_secret: <api_key_token>
data_platform: <data_platform>  # SNOWFLAKE, BIGQUERY, REDSHIFT, etc.
```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Snowflake Account

If some of the monitored data assets are Snowflake datasets, please provide the Snowflake account as follows,

```yaml
snowflake_account: <account_name>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `monte_carlo` extra.

Run the following command to test the connector locally:

```shell
metaphor monte_carlo <config_file>
```

Manually verify the output after the command finishes.
