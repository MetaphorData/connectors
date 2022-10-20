# Synapse Connector

This connector extracts technical metadata from Azure Synapse workspaces using [Synapse API](https://learn.microsoft.com/en-us/rest/api/synapse/)

## Setup

We recommend creating a dedicated Azure AD Application and a dedicated security group for the connector to use.

1. 1. Follow [Step 1 of this doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-1---create-an-azure-ad-app) to create an Azure AD app and a client secret.


<!-- Pending to write and limit necessary permission for api -->
2. Access control in Azure Portal
   - asure workspace
   - storage account
     - IAM: add storage Queue Data Contributor

5. Access control in Microsoft Azure Synapse Portal, go to your 

  - 
## Config File

Create a YAML config file based on the following template.

### Required Configurations
To speceify workspaces to crawl, need to also assign **Resource Group Name**. Otherwise, the connector will crawl all authorized synapse workspaces. 

```yaml
tenant_id: <tenant_id>  # The azure directory (tenant) id

client_id: <client_id>  # The azure application client id

secret: <secret>  # The azure application client secret

subscription_id: <subscription_id> # The Azure Subscription id

resource_group_name: <resource_group_name> # (Optinal) Rescource group name

workspaces: # (Optional) The workspace names
  - <workspace1>
  - <workspace2>
  ...

output:
  file:
    directory: <output_directory> # the output result directory
