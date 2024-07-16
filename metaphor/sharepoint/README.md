# Sharepoint Connector

This connector extracts search documents from pages on Sharepoint sites. It retrieves all pages on all sites that it has access to.

## Setup

This connector makes use of a Microsoft Azure [App Registration](https://learn.microsoft.com/en-us/security/zero-trust/develop/app-registration). Ensure that you apply either the `Microsoft Graph / Sites.Read.All` permission or configure `Microsoft Graph / Sites.Selected` appropriately to allow access to the desired site(s).

Configure the client ID, client secret, and tenant ID using the information from the registered App.

Additionally, this connector requires [Azure OpenAI services](https://azure.microsoft.com/en-us/products/ai-services/openai-service) or an [OpenAI API key](https://platform.openai.com) to generate embedding vectors for documents.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
sharepoint_client_id: sharepoint_client_id
sharepoint_client_secret: sharepoint_client_secret
sharepoint_tenant_id: sharepoint_tenant_id

embedding_model:
  azure_openai:
    key: <key>
    endpoint: <endpoint>
```

Note that an embedding model needs to be appropriately configured. This example shows how to configure an Azure OpenAI services model, but you can use other [supported models](/docs/embeddings.md).

### Optional Configurations

These defaults are provided; you don't have to manually configure them.

`include_text` specifies whether to include the original document text alongside the embedded content.

```yaml
embedding_model:  # in the same block as above
  azure_openai:
    version: <version> # "2024-03-01-preview"
    deployment_name: <deployment_name> # "Embedding_3_small"
    model: <model> # "text-embedding-3-small"
  chunk_size: 512
  chunk_overlap: 50

include_text: <include_text> # False
```

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include the `sharepoint` or `all` extra.

Run the following command to test the connector locally:

```shell
metaphor sharepoint <config_file>
```

Manually verify the output after the run finishes.