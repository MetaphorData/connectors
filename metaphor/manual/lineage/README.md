# Manual Lineage Connector

This connector reads manually specified dataset lineages from a config file.

## Setup

No special setup is required.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
lineages:
  - dataset:
      platform: <data_platform>
      name: <dataset_name>
    upstreams:
      - platform: <dataset_platform>
        name: <dataset_name>
        account: <snowflake_account> # only for Snowflake
      ...
  ...
output:
  file:
    directory: <output_directory>
```

> Note: You only need to specify `account` if the platform is `SNOWFLAKE`.

See [Common Configurations](../common/README.md) for more information on `output`.

### Examples

Here's an example specifying that BigQuery `project.db.table1` sources its data from `project.db.table2` & `project.db.table3`:

```yaml
lineages:
  - dataset:
      platform: BIGQUERY
      name: project.db.table1
    upstreams:
      - platform: BIGQUERY
        name: project.db.table2
      - platform: SNOWFLAKE
        name: db.schema.table3
        account: snowflake_account
output:
  file:
    directory: /output
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

To test the connector locally, change the config file to output to a local path and run the following command

```shell
python -m metaphor.manual.lineage <config_file>
```

Manually verify the output after the run finishes.
