# Custom Metadata Connector

This connector attaches custom metadata to tables.

## Setup

No special setup is required.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
datasets:
  - id:
      platform: <data_platform>
      name: <dataset_name>
      account: <snowflake_account> # only for Snowflake
    metadata:
      <key>: <value>
      ...
  ...
output:
  file:
    directory: <output_directory>
```

> Note: You only need to specify `account` if the platform is `SNOWFLAKE`.

See [Output Config](../../common/docs/output.md) for more information on `output`.

### Examples

Here's an example showing how to attach custom metadata that contains primitive or complex values to a BigQuery table:

```yaml
datasets:
  - id:
      platform: BIGQUERY
      name: database.schema.table1
    metadata:
      key1: string_value
      key2: 2
      key3:
        - item1
        - item2
      key4:
        f1: value1,
        f2: value2
output:
  file:
    directory: /output
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor manual.metadata <config_file>
```

Manually verify the output after the run finishes.
