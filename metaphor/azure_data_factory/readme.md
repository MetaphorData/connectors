# Azure Data Factory Connector

This connector extracts technical metadata from Azure Data Factories using [Azure REST API](https://learn.microsoft.com/en-us/rest/api/datafactory/v2)

## Setup

We recommend creating a dedicated Azure AD Application for the connector to use.

1. Follow [Step 1 of this doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-1---create-an-azure-ad-app) to create an Azure AD app and a client secret.

2. Give the app a reader role to access REST APIs. Go to the resource you want to connect to. For example, a subscription. On the resource page, select `Access Control (IAM)`, and add the built-in `Reader` role to the app you created in Step 1.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
tenant_id: <tenant_id>  # The Azure Directory (tenant) ID

client_id: <client_id>  # The Azure Application client id

client_secret: <client_secret>  # The client secret value (not secret ID)

subscription_id: <subscription_id>  # Azure subscription id

output:
  file:
    directory: <output_directory>
```


## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `datafactory` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor azure_data_factory <config_file>
```

Manually verify the output after the command finishes.
