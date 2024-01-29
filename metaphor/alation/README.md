# DataHub Connector

This connector extracts metadata for user generated from Alation.

## Setup

To run the connector, you must have a set of API token. Follow [the official documentation](https://developer.alation.com/dev/docs/authentication-into-alation-apis) to generate such a token.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
url: <url to the Alation instance>
token: <API token>
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
metaphor alation <config_file>
```

Manually verify the output after the run finishes.
