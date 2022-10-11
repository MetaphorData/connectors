# Synapse Connector

This connector extracts technical metadata from Azure Synapse workspaces using [Synapse API](https://learn.microsoft.com/en-us/rest/api/synapse/)

## Setup

We recommend creating a dedicated Azure AD Application and a dedicated security group for the connector to use.

1. Follow [Step 1 of this doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-1---create-an-azure-ad-app) to create an Azure AD app and a client secret.

2. Follow [this doc](https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account) to create Data Lake Storage Gen2 which set as the default storage warehouse for synapse in the next step.

3. Follow [this doc](https://learn.microsoft.com/en-us/azure/synapse-analytics/quickstart-create-workspace) to create a synapse workspace for you.

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
