# Synapse Connector

This connector extracts technical metadata from Azure Synapse workspaces using [Synapse API](https://learn.microsoft.com/en-us/rest/api/synapse/).

## Setup

We recommend creating a dedicated Azure AD Application and a dedicated security group for the connector to use.

1. Follow [Step 1 of this doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-1---create-an-azure-ad-app) to create an Azure AD app and a client secret.

2.  Follow steps to set up/check enough permissions in Azure portal to perform Azure Synapse connector:
    1. Access control in Azure Portal, go to your Azure Synapse workspace in [Azure Portal](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Synapse%2Fworkspaces).
    2. Click the workspace which will be used in the Synapse connector.
    3. Click `Access control(IAM)` tab on the left-hand panels.
    4. Click `+Add` and select `Add role assignment` to add role-based permissions to the AD app we created in the first step.
    5. Add build-in role `Reader` or custom role to the app and wait for a few minutes for applied to all resources.

       - the build-in `Reader` role has more permissions than required by the connector. To limit the permission, you can create a custom role with only `"Microsoft.Synapse/*/read"` permission.  
       Sample permission JSON file as follow:
          ```json
          {
            "id": "<role_id>",
            "properties": {
                  "roleName": "",
                  "description": "",
                  "assignableScopes": [""],
                  "permissions": [
                      {
                          "actions": [
                            "Microsoft.Synapse/*/read"
                          ],
                          "notActions": [],
                          "dataActions": [],
                          "notDataActions": []
                      }
                  ]
            }
          }
          ```

3. Follow the steps to set up/check enough permissions in Microsoft Azure Synapse portal to perform Azure Synapse connector:
    1. Go to your synapse workspace portal.
       - if you cannot find it, go to [Azure Portal](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Synapse%2Fworkspaces) then click the workspace and you could find Workspace web URL in `Overview` section.
    2. Click `Manage` on the left-hand panel.
    3. Select `Access control` (Security -> Access control).
    4. Click `+ Add` then add role-based permissions to the AD app we created in the first step.
    5. Role select `Synapse Monitoring Operator`.
    6. Wait for a few minutes for permission apply and then could start to use Synapse connector.

4. Setup Microsoft pymssql library
    1. Set up SQL admin username and admin password for Synapse workspace in [Azure portal](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Synapse%2Fworkspaces).
    2. Follow the tutorial to install [pymssql](https://learn.microsoft.com/en-us/sql/connect/python/pymssql/step-1-configure-development-environment-for-pymssql-python-development?view=sql-server-ver16)
    3. (Optional) may need to install [FreeTDS](https://learn.microsoft.com/en-us/sql/connect/python/pymssql/step-1-configure-development-environment-for-pymssql-python-development?view=sql-server-ver16) if running into errors.
    4. (Optional) if want to set up different username and password for Synapse connector follow this [tutorial](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/sql-authentication?tabs=serverless)
    5. (Optional) Apple Silicon users may encounter build error when installing `pymssql` follow [apple_silicon](https://github.com/MetaphorData/connectors/blob/main/docs/apple_silicon.md)

5. (Optional) Enable the query log by setting `lookback_days` in the config file

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
tenant_id: <tenant_id>  # The azure directory (tenant) id

client_id: <client_id>  # The azure application client id

secret: <secret>  # The azure application client secret

subscription_id: <subscription_id>  # The Azure Subscription id

workspace_name: <workspace_name> # The Microsoft Synapse workspace name

resource_group_name: <resource_group_name>  # Rescource group name

username: <username> # The synapse workspace SQL username

password: <password> # The Synapse workspace SQL password

output:
  file:
    directory: <output_directory>  # the output result directory
```

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations
#### Query Log
```yaml
query_log:
  lookback_days: <days>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `synapse` extra.

To test the connector locally, change the config file to output to a local path and run the following command.

```shell
metaphor synapse <config_file>
```

Manually verify the output after the command finishes.
