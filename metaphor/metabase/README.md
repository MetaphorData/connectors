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
output:
  file:
    directory: <output_directory>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `metabase` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor metabase <config_file>
```

Manually verify the output after the command finishes.
