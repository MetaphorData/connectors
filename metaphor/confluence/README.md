# Confluence Connector

This connector extracts documents from Atlassian [Confluence](https://confluence.atlassian.com/) Cloud, Data Center, or Server (given appropriate authentication configuration). For Confluence Cloud, authentication is supported via username / API token. For Confluence Data Center / Server, authentication is supported either by username / access token or a personal access token (PAT).

## Setup

A service account username and API token is required for this connector. Follow [these instructions](https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/) to create and retrieve the token, or follow [these instructions](https://confluence.atlassian.com/enterprise/using-personal-access-tokens-1026032365.html) to create and retrieve a PAT for Data Center / Server installations.

Ensure that the service acccount whose token is being used has the appropriate access to the spaces and pages that you would like to extract content from.

Additionally, this connector requires [Azure OpenAI services](https://azure.microsoft.com/en-us/products/ai-services/openai-service) to generate embedding vectors for documents.

## Config File

Create a YAML config file based on the following template.

One of the authentication methods must be appropriately configured (either username / token OR PAT, depending on the version of Confluence you are connecting to).

Additionally, *one* of the page filtering options must be appropriately configured, any one of the following. The configured entry should match the `select_method`:
* `space_key` - load all pages from a space
  *  `page_status` can be optionally configured as `None`, `current`, `archived`, `draft`
* `page_ids` - load all pages from a list of ids
  *  `include_children` can be optionally configured to load all child pages as well
* `label` - load all pages with a given label
* `cql` - load all pages that match a given CQL query
  * More about CQL [here](https://developer.atlassian.com/server/confluence/advanced-searching-using-cql/).

The `space_key` and `page_id` of a given page can be found in the URL:
```
https://confluence.company.com/wiki/spaces/<space_key>/pages/<page_id>/...
```

### Required Configurations

```yaml
# General Confluence configs
confluence_base_URL: <confluence_base_URL> # Ends in "/wiki", e.g. "https://confluence.company.com/wiki"; no trailing "/"
confluence_cloud: <is_confluence_cloud> # True or False
select_method: <select_method> # one of "space_key", "page_ids", "cql"

# for Confluence Cloud
confluence_username: <confluence_username> # service account username, "name@company.com" usually
confluence_token: <confluence_token> # service account API token

# for Confluence Data Center or Server
confluence_PAT: <confluence_PAT> # service account Personal Access Token

# PICK ONE OF space_key OR page_ids OR label OR cql
space_key: <space_key> # ex: "KB"

page_ids: <page_ids> # ex: [1234, 5678]

label: <label> # ex: "my-label"

cql: <cql> # ex: "space = DEV and creator not in (Jack,Jill,John)"

# Azure OpenAI configs
azure_openAI_key: <azure_openAI_key>
azure_openAI_endpoint: <azure_openAI_endpoint>
```

### Optional Configurations
`include_attachments` specifies whether to parse files attached to pages that are retrieved by the connector. Supported filetypes are PDF, PNG, JPEG/JPG, SVG, Word and Excel.

`include_children` works when `page_ids` are configured above, refers to if you'd like to parse  child pages.

`page_status` works when `space_key` is configured above, refers to choosing only a specific page type

`include_text` specifies whether to include the original document text alongside the embedded content.
```yaml
include_attachments: <include_attachments> # False

include_children: <include_children> # False

page_status: <page_status> # "current"

azure_openAI_version: <azure_openAI_version> # "2024-03-01-preview"
azure_openAI_model_name: <azure_openAI_model_name> # "Embedding_3_small"
azure_openAI_model: <azure_openAI_model> # "text-embedding-3-small"

include_text: <include_text> # False
```

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include the `confluence` or `all` extra.

Run the following command to test the connector locally:

```shell
metaphor confluence <config_file>
```

Manually verify the output after the run finishes.