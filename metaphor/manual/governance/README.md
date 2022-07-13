# Manual Governance Connector

This connector assigns manually specified ownership and tagging information to tables.

## Setup

No special setup is required.

## Config File

Create a YAML config file based on the following template.

> Note: The ownership types and tags must be first created on Metaphor for the assignment to take effect.

### Required Configurations

```yaml
datasets:
  - id:
      platform: <data_platform>
      name: <dataset_name>
      account: <snowflake_account> # only for Snowflake
    ownerships:
      - type: <ownership_type>
        email: <owner_email>
      ...
    tags:
      - <tag_name>
      ...
  ...
output:
  file:
    directory: <output_directory>
```

> Note: You only need to specify `account` if the platform is `SNOWFLAKE`.

See [Common Configurations](../common/README.md) for more information on `output`.

### Examples

Here's an example on how to assign `alice@test.com` and `bob@test.com` as owners of a BigQuery table.

```yaml
datasets:
  - id:
      platform: BIGQUERY
      name: project.db.table1
    ownerships:
      - type: Data Steward
        email: bob@test.com
      - type: 
        email: alice@test.com
output:
  file:
    directory: /output
```

Here's another example showing how to tag a Snowflake table as `pii` and `golden`.

```yaml
datasets:
  - id:
      platform: BIGQUERY
      account: test_account
      name: database.schema.table1
    tags:
      - pii
      - golden 
output:
  file:
    directory: /output
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

To test the connector locally, change the config file to output to a local path and run the following command

```shell
python -m metaphor.manual.governance <config_file>
```

Manually verify the output after the run finishes.
