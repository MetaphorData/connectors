# Metabase Connector

This connector extracts technical metadata from Metabase using [Metabase API](https://www.metabase.com/learn/administration/metabase-api.html).

## Setup

We recommend creating a dedicated Metabase user and role with limited permission for the connector to use.

1. Log into Metabase as an Administrator.
2. Go to `People` tab, click `Create Users` and fill out the name and email. An email should be sent to the user. 
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
    path: <path_to_output_file>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `metabase` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
python -m metaphor.metabase <config_file>
```

Manually verify the output after the command finishes.
