# Notion Connector

This connector extracts documents from an appropriately configured Notion site using the [Notion API](https://developers.notion.com/reference/intro) and the [LlamaIndex loader](https://llamahub.ai/l/notion) for Notion.

## Setup

A Notion integration's API token is required for this connector. Follow [these instructions](https://developers.notion.com/docs/create-a-notion-integration#create-your-integration-in-notion) to create a Notion integration and retrieve the associated API secret. 

The integration will only have access to pages that it has been added to, or pages whose parent it has been added to. To ensure a certain section or group of pages is crawled, add the integration to the parent of those pages by following [these instructions](https://developers.notion.com/docs/create-a-notion-integration#give-your-integration-page-permissions).

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
api_key_token: <api_key_token>
mongo_uri: <mongo_uri>
output:
  file:
    directory: <output_directory> 
```

### Optional Configurations

```yaml
api_key_version: <api_key_version> # "2022-06-08" by default
embedding_chunk_size: <embedding_chunk_size> # 512 by default
embedding_overlap_size: <embedding_overlap_size> # 50 by default
```