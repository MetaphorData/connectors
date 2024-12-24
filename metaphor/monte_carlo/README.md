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

#### Treat Unhandled Anomalies as Errors

If set to true, the connector will treat unhandled [anomalies](https://docs.getmontecarlo.com/docs/detectors-overview) as data quality errors. Default is false.

```yaml
treat_unhandled_anomalies_as_errors: true
```

#### Anomalies Lookback Days

By default the connector only retrieves anomalies from the last 30 days. You can change this by setting the `anomalies_lookback_days` field.

```yaml
anomalies_lookback_days: 30
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `monte_carlo` extra.

Run the following command to test the connector locally:

```shell
metaphor monte_carlo <config_file>
```

Manually verify the output after the command finishes.
