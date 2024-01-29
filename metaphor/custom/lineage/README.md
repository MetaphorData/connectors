# Custom Lineage Connector

This connector reads custom dataset lineages from a config file.

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
```

> Note: You only need to specify `account` if the platform is `SNOWFLAKE`.

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

### Examples

Here's an example specifying that BigQuery table `project.db.table1` sources its data from BigQuery table `project.db.table2` & Snowflake table `db.schema.table3`:

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
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

Run the following command to test the connector locally:

```shell
metaphor custom.lineage <config_file>
```

Manually verify the output after the run finishes.
