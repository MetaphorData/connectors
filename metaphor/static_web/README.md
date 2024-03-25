# Static Webpage Connector

This connector extracts search documents from publically accessible webpages. It crawls pages by recursively visiting pages of the same domain with adjustable depth.

## Setup

Specify a list of target pages that should be crawled along with desired recursion depths. Be careful with a recursion depth greater than 1.

## Config File

Create a YAML config file based on the following template.

`depth = 1` corresponds to scraping the specified page and its subpages only. Higher configured depths will recursively perform the same action on subpages `n` times.

### Required Configurations

```yaml
azure_openAI_key: <azure_openAI_key>
azure_openAI_endpoint: <azure_openAI_endpoint>

links: []
depths: []

output:
  file:
    directory: <output_directory>
```

### Optional Configurations
```yaml
azure_openAI_version: <azure_openAI_version> # "2024-03-01-preview"
azure_openAI_model_name: <azure_openAI_model_name> # "Embedding_3_small"
azure_openAI_model: <azure_openAI_model> # "text-embedding-3-small"

include_text: <include_text> # True
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `static_web` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor static_web <config_file>
```

Manually verify the output after the command finishes.
