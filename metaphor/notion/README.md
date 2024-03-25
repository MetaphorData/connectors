# Notion Connector

This connector extracts documents from an appropriately configured Notion site using the [Notion API](https://developers.notion.com/reference/intro) and the [LlamaIndex loader](https://llamahub.ai/l/notion) for Notion. It relies on a configured Azure OpenAI embedding model to get the appropriate vector embeddings for input documents.

## Setup

A Notion integration's API token is required for this connector. Follow [these instructions](https://developers.notion.com/docs/create-a-notion-integration#create-your-integration-in-notion) to create a Notion integration and retrieve the associated API secret. 

The integration will only have access to pages that it has been added to, or pages whose parent it has been added to. To ensure a certain section or group of pages is crawled, add the integration to the parent of those pages by following [these instructions](https://developers.notion.com/docs/create-a-notion-integration#give-your-integration-page-permissions).

Additionally, this connector requires [Azure OpenAI services](https://azure.microsoft.com/en-us/products/ai-services/openai-service) to generate embedding vectors for documents.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
notion_api_token: <notion_api_token>

azure_openAI_key: <azure_openAI_key>

azure_openAI_endpoint: <azure_openAI_endpoint>
```

### Optional Configurations

These defaults are provided; you don't have to manually configure them.

`include_text` refers to if you'd like to include the original document text alongside the embedded content.

```yaml
azure_openAI_version: <azure_openAI_version> # "2024-03-01-preview"
azure_openAI_model_name: <azure_openAI_model_name> # "Embedding_3_small"
azure_openAI_model: <azure_openAI_model> # "text-embedding-3-small"

notion_api_version: <api_key_version> # "2022-06-28"
include_text: <include_text> # False
```

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include the `notion` or `all` extra.

Run the following command to test the connector locally:

```shell
metaphor notion <config_file>
```

Manually verify the output after the run finishes.