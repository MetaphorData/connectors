# Informatica Connector

This connector extracts technical metadata from Informatica using [Informatica Intelligent Cloud Services REST API](https://docs.informatica.com/integration-cloud/b2b-gateway/current-version/rest-api-reference/preface.html).

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
base_url: <base_url>
user: <username>
password: <password>
```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

Run the following command to test the connector locally:

```shell
metaphor informatica <config_file>
```

Manually verify the output after the command finishes.
