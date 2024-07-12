# Monday Connector

This connector extracts documents from [Monday.com](https://monday.com/) boards. It requires an appropriately configured API key.

## Setup

A service account's or personal account's API key is required for this connector. Follow [these instructions](https://support.monday.com/hc/en-us/articles/360005144659-Does-monday-com-have-an-API) to create and retrieve the key.

Ensure that the service acccount whose API key is being used has the appropriate access to the boards that you want to extract documents from. This connector will discover and retrieve items from boards its associated API key has access to.

Additionally, this connector requires [Azure OpenAI services](https://azure.microsoft.com/en-us/products/ai-services/openai-service) or an [OpenAI API key](https://platform.openai.com) to generate embedding vectors for documents.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
monday_api_key: <monday_api_key>
monday_api_version: <monday_api_version>

embedding_model:
  azure_openai:
    key: <key>
    endpoint: <endpoint>
```

Note that an embedding model needs to be appropriately configured. This example shows how to configure an Azure OpenAI services model, but you can use other [supported models](/docs/embeddings.md).

### Optional Configurations

These defaults are provided; you don't have to manually configure them.

`include_text` refers to if you'd like to include the original document text alongside the embedded content.

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

Follow the [installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include the `monday` or `all` extra.

Run the following command to test the connector locally:

```shell
metaphor monday <config_file>
```

Manually verify the output after the run finishes.