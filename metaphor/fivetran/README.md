# FiveTran Connector

This connector extracts technical metadata from FiveTran using [FiveTran REST API](https://fivetran.com/docs/rest-api).

## Setup

To access the FiveTran REST API, an API Key and an API secret are needed. You can follow the [FiveTran documentation](https://fivetran.com/docs/rest-api/getting-started) to generate a key and a secret.


## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
api_key: <api_key>
api_secret: <api_secret>
```

### Optional Configurations

#### Output Destination

By default, the connector writes the extracted metadatas to `${pwd}/${CURRENT_TIMESTAMP}`. To modify the location or disable writing altogether, see [Output Config](../common/docs/output.md) for more information.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor fivetran <config_file>
```

Manually verify the output after the command finishes.
