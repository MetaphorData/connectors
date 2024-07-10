# Embeddings Connectors Documentation

Metaphor develops several knowledgebase connectors that are capable of retrieving documents and generating vector embeddings for use with Metaphor AI.

# Supported Endpoints

Embedding models are configured via the `EmbeddingModelConfig` [class](/metaphor/common/embeddings_config.py). Required and optional configs should be entered in the `embed_model_config` dictionary in the crawler YAML file.

## Azure OpenAI service models
[Home](https://azure.microsoft.com/en-us/products/ai-services/openai-service)

The following models are known to work:
* `text-embedding-ada-002`
* `text-embedding-3-small`

### Configuration

```yaml
embed_model_config:
  # REQUIRED CONFIGS
  azure_openAI_key: <azure_openAI_key>
  azure_openAI_endpoint: <azure_openAI_endpoint>
  # DEFAULTS; DON'T NEED TO BE CONFIGURED
  azure_openAI_version: <azure_openAI_version> # "2024-03-01-preview"
  azure_openAI_model_name: <azure_openAI_model_name> # "Embedding_3_small"
  azure_openAI_model: <azure_openAI_model> # "text-embedding-3-small"
  chunk_size: 512
  chunk_overlap: 50
```

## OpenAI API models
[Home](https://platform.openai.com)

The following models are known to work:
* `text-embedding-ada-002`
* `text-embedding-3-small`

### Configuration

```yaml
embed_model_config:
  # REQUIRED CONFIGS
  openAI_key: <openAI_key>
  # DEFAULTS; DON'T NEED TO BE CONFIGURED
  openAI_model: <openAI_model> # "text-embedding-3-small"
  chunk_size: 512
  chunk_overlap: 50
```

# Other Configuration

Configuration of the `chunk_size` and `chunk_overlap` is supported as well since some models have smaller context windows and for optimizing search detail.
