# Power BI Connector

This connector extracts technical metadata from Power BI workspaces using [Read-only Power BI admin APIs](https://docs.microsoft.com/en-us/power-bi/enterprise/read-only-apis-service-principal-authentication).

## Setup

We recommend creating a dedicated Azure AD Application and a dedicated security group for the connector to use.

1. Follow [Step 1 of this doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-1---create-an-azure-ad-app) to create an Azure AD app and a client secret.

    > Note: Make sure to NOT add any Power BI Service permissions to the app. Doing so will lead to authentication errors when calling the APIs. See [this notice](https://docs.microsoft.com/en-us/power-bi/enterprise/read-only-apis-service-principal-authentication#:~:text=Make%20sure%20there%20are%20no%20Power%20BI%20admin%2Dconsent%2Drequired%20permissions%20set%20on%20this%20application.%20For%20more%20information%2C%20see%20Managing%20consent%20to%20applications%20and%20evaluating%20consent%20requests.) for more details.

2. (Optional) To get the sensitivity labe information. Follow the steps to ddd `InformationProtectionPolicy.Read.All` to the dedicated Azure AD application.
    1. Sign into the Azure portal
    2. Select Azure Active Directory, then Enterprise applications.
    3. Select the application you created
    4. Select `Permissions`, and `Add a permission`
    5. Select `Microsoft Graph` -> `Application permissions`
    6. Find `InformationProtectionPolicy.Read.All` and click add button below.
    7. Grant the permission you select.

3. Follow [this doc](https://docs.microsoft.com/en-us/azure/active-directory/fundamentals/active-directory-groups-create-azure-portal#create-a-basic-group-and-add-members) to create a security group and add the app's service principal as a member.

4. Log into [Power BI Admin Portal](https://app.powerbi.com/admin-portal/tenantSettings) as a Power BI admin, enable the following settings under **Admin API settings** for the security group created in the previous step:
    - Allow service principals to use Power BI APIs
    - Allow service principals to use read-only Power BI admin APIs
    - Enhance admin APIs responses with detailed metadata
    - Enhance admin APIs responses with DAX and mashup expressions

For example,

![](https://docs.microsoft.com/en-us/power-bi/enterprise/media/read-only-apis-service-principal-auth/allow-service-principals-tenant-setting.png)

4. [Add the service principal](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-4---add-the-service-principal-to-your-workspace) to all workspaces of interest, except "My Workspaces". Note that it may take an hour before the permission is fully propagated.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
tenant_id: <tenant_id>  # The Power BI tenant ID

client_id: <client_id>  # The Azure Application client id

secret: <secret>  # The client secret value (not secret ID)
```

### Optional Configurations

By default, the connector will connect all workspaces under a tenant (organization). You can explicitly configure workspaces you want to connect.

```yaml
workspaces:
  - <workspace_id>  # The workspace id
  - <workspace_id>  # The workspace id
```

When extracting lineages, the connector parses the snowflake account directly from the [DAX expression](https://learn.microsoft.com/en-us/dax/). In the rare cases when it's unable to do so, the parser will fall back to the default snowflake account specified in the config file as follows.

```yaml
snowflake_account: <snowflake_account>
```

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `power_bi` extra.

Run the following command to test the connector locally:

```shell
metaphor power_bi <config_file>
```

Manually verify the output after the command finishes.
