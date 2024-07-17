# Static Webpage Connector

This connector extracts search documents from publically accessible webpages. It crawls pages by recursively visiting pages of the same domain with adjustable depth.

## Setup

Specify a list of target pages that should be crawled along with desired recursion depths. Be careful with a recursion depth greater than 1.

Additionally, this connector requires [Azure OpenAI services](https://azure.microsoft.com/en-us/products/ai-services/openai-service) or an [OpenAI API key](https://platform.openai.com) to generate embedding vectors for documents.

## Config File

Create a YAML config file based on the following template.

`depth = 1` corresponds to scraping the specified page and its subpages only. Higher configured depths will recursively perform the same action on subpages `n` times.

### Required Configurations

```yaml
links: []
depths: []

embedding_model:
  azure_openai:
    key: <key>
    endpoint: <endpoint>
```

Note that an embedding model needs to be appropriately configured. This example shows how to configure an Azure OpenAI services model, but you can use other [supported models](/docs/embeddings.md).

### Optional Configurations
```yaml
embedding_model:  # in the same block as above
  azure_openai:
    version: <version> # "2024-03-01-preview"
    deployment_name: <deployment_name> # "Embedding_3_small"
    model: <model> # "text-embedding-3-small"
  chunk_size: 512
  chunk_overlap: 50

include_text: <include_text> # True
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `static_web` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor static_web <config_file>
```

Manually verify the output after the command finishes.
