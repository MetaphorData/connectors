# BigQuery Data Profiling Connector

This connector extracts column-level data profiles from a BigQuery project using [Python Client for Google BigQuery](https://googleapis.dev/python/bigquery/latest/index.html).

## Setup

Create a [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) based on the [Setup](../README.md#Setup) guide for the general BigQuery connector. You'll need to grant additional permissions to the account to perform profiling. You can add the `BigQuery Job User` and `BigQuery Data Viewer` roles to your service account.

## Config File

The config file inherits all the required and optional fields from the general BigQuery connector [Config File](../README.md#config-file). In addition, you can specify the following configurations:

### Optional Configurations

#### Column Statistics

See [Column Statistics](../../common/docs/column_statistics.md) for details.

#### Sampling

See [Sampling Config](../../common/docs/sampling.md) for details.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `bigquery` extra.

Run the following command to test the connector locally:

```bash
metaphor bigquery.profile <config_file>
```

Manually verify the output after the run finishes.
