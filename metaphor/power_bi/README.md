# PowerBI Connector

This connector extracts technical metadata from PowerBI workspaces using [Power BI REST APIs](https://docs.microsoft.com/en-us/rest/api/power-bi/).

## Setup

We recommend creating a dedicated Azure AD Application and a dedicated security group for the connector to use.

1. Follow [Microsoft doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal) to create an app and a security group. Add the app's [service principal](https://docs.microsoft.com/en-us/azure/active-directory/develop/app-objects-and-service-principals#service-principal-object) to the security group.

2. Login to PowerBI as Admin ane enable the following setting.
    - Allow service principles to use Power BI APIs
    - Allow service principals to use read-only Power BI admin APIs
    - Enhance admin APIs responses with detailed metadata

### Note

1. Make sure you add either service principal or security group to your workspaces.

2. Make sure your app did not have any extra permission granted. Simply add the service principal to your workspace. See [Enable service principal authentication for read-only admin APIs](https://docs.microsoft.com/en-us/power-bi/admin/read-only-apis-service-principal-authentication) for more details.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
tenant_id: <tenant_id>  # The PowerBI tenant ID

workspace_id: <workspace_id>  # The workspace the connector will connect to

client_id: <client_id>  # The Azure Application client id

secret: <secret>  # The client secret

output:
  file:
    directory: <output_directory>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `power_bi` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
python -m metaphor.power_bi <config_file>
```

Manually verify the output after the command finishes.
