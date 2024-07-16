# Embeddings Connectors Documentation

Metaphor develops several knowledgebase connectors that are capable of retrieving documents and generating vector embeddings for use with Metaphor AI.

# Supported Endpoints

Embedding models are configured via the `EmbeddingModelConfig` [class](/metaphor/common/embeddings_config.py). Required and optional configs should be entered in the `embedding_model` dictionary in the crawler YAML file. See below for formatting examples.

## Azure OpenAI service models
[Home](https://azure.microsoft.com/en-us/products/ai-services/openai-service)

The following models are known to work:
* `text-embedding-ada-002`
* `text-embedding-3-small`

### Configuration

```yaml
embedding_model:
  azure_openai:
    key: <key>  # Required
    endpoint: <endpoint>  # Required
    version: <version> # Defaults to "2024-03-01-preview" if not specified
    deployment_name: <deployment_name> # Defaults to "Embedding_3_small" if not specified
    model: <azure_openAI_model> # Defaults to "text-embedding-3-small" if not specified
  chunk_size: 512  # Defaults to 512
  chunk_overlap: 50  # Defaults to 50
```

## OpenAI API models
[Home](https://platform.openai.com)

The following models are known to work:
* `text-embedding-ada-002`
* `text-embedding-3-small`

### Configuration

```yaml
embedding_model:
  openai:
    key: <openAI_key>  # Required
    model: <openAI_model> # Defaults to "text-embedding-3-small" if not specified
  chunk_size: 512  # Defaults to 512
  chunk_overlap: 50  # Defaults to 50
```

# Other Configuration

Configuration of the `chunk_size` and `chunk_overlap` is supported as well since some models have smaller context windows and for optimizing search detail. There are defaults configured.
