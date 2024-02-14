# Sharepoint Connector



## Setup



Additionally, this connector requires [Azure OpenAI services](https://azure.microsoft.com/en-us/products/ai-services/openai-service) to generate embedding vectors for documents.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml


azure_openAI_key: <azure_openAI_key>

azure_openAI_endpoint: <azure_openAI_endpoint>
```

### Optional Configurations

These defaults are provided; you don't have to manually configure them.

`include_text` refers to if you'd like to include the original document text alongside the embedded content.

```yaml
azure_openAI_version: <azure_openAI_version> # "2023-12-01-preview"
azure_openAI_model_name: <azure_openAI_model_name> # "Embedding_ada002"
azure_openAI_model: <azure_openAI_model> # "text-embedding-ada-002"

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