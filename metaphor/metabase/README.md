# Metabase Connector

This connector extracts technical metadata from Metabase using [Metabase API](https://www.metabase.com/learn/administration/metabase-api.html).

## Setup

In order to fetch dashboards, charts and lineage information, an Administrator user and credential is needed. We recommend creating a dedicated Metabase user and role for the connector to use. The reason we need Administrator permission is to read the database details, so we can link charts to upstream dataset entities in Metaphor. Without Administrator permission, the connector can still be run and fetch dashboards and charts, without upstream lineage information.

To create a new Metabase user:
1. Log into Metabase as an Administrator.
2. Go to `People` tab, click `Create Users`, fill out the name and email, and choose `Admin` group. An email should be sent to the user. 
3. Follow the URL link in the email to activate user and password, and login to metabase.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
server_url: <metabase_server_url>  // e.g. "https://xxx.metabaseapp.com" for Metabase Cloud
username: <username>
password: <password>
```

### Optional Configurations

#### Database Defaults

Metabase's API does not provide information on the default schema used to execute [native queries](https://www.metabase.com/glossary/native_query). This makes it difficult to parse the lineage precisely. When this happens, use `database_defaults` to manually set the [database](https://www.metabase.com/docs/latest/databases/start)'s default schema:

```yaml
database_defaults:
    - id: <id of the database in Metabase>
      default_schema: <default schema for the database>
```

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Excluding Directories

You can specify the directories (collection paths) to be included / excluded by the connector. By default all assets are included.

To specify the directories to include / exclude, use the following field:

```yaml
filter:
  includes:
    - top_level_directory
    - directory/sub_directory
    ...
  excludes:
    - top_level_directory2
    - directory/sub_directory2/sub_sub_dir
    ...
```

To only include specific paths, use `includes` field. To only exclude certain paths, use `excludes` field.


## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `metabase` extra.

Run the following command to test the connector locally:

```shell
metaphor metabase <config_file>
```

Manually verify the output after the command finishes.
