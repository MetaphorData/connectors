# Static Webpage Connector

## Setup

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
output:
  file:
    directory: <output_directory>
```

### Optional Configurations

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `static_web` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor static_web <config_file>
```

Manually verify the output after the command finishes.
