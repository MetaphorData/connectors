# Azure Data Factory Connector

This connector extracts technical metadata from Azure Data Factories using [Azure REST API](https://learn.microsoft.com/en-us/rest/api/datafactory/v2)

## Setup

We recommend creating a dedicated Azure AD Application for the connector to use.

1. Follow [Step 1 of this doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-1---create-an-azure-ad-app) to create an Azure AD app and a client secret.

2. Grant the app the [Reader Role](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#reader) to all data factories of interest. Select `Access Control (IAM)` from each data factory's settings page and add the built-in `Reader` role to the app you created in Step 1.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
tenant_id: <tenant_id>  # The Azure Directory (tenant) ID

client_id: <client_id>  # The Azure Application client id

client_secret: <client_secret>  # The client secret value (not secret ID)

subscription_id: <subscription_id>  # Azure subscription id

```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `datafactory` extra.

Run the following command to test the connector locally:

```shell
metaphor azure_data_factory <config_file>
```

Manually verify the output after the command finishes.
