# Synapse Connector

This connector extracts technical metadata from Azure Synapse workspaces using [Synapse API](https://learn.microsoft.com/en-us/rest/api/synapse/)

## Setup

We recommend creating a dedicated Azure AD Application and a dedicated security group for the connector to use.

1. Follow [this doc](https://learn.microsoft.com/en-us/azure/synapse-analytics/quickstart-create-workspace) to create a synapse workspace for you.

<!-- Pending to write and limit necessary permission for api   -->
2. Access control in Azure Portal
   - asure workspace
   - storage account
     - IAM: add storage Queue Data Contributor

5. Access control in Microsoft Azure Synapse Portal

   - 
## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
tenant_id: <tenant_id>  # The Power BI tenant ID

client_id: <client_id>  # The Azure Application client id

secret: <secret>  # The client secret

subscription_id: <subscription_id> # The Azure Subscription id

output:
  file:
    directory: <output_directory> # the output result directory
