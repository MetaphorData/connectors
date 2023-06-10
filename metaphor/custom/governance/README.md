# Custom Governance Connector

This connector assigns custom ownership, tagging information, and descriptions to tables.

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
    column_tags:
      - column: <column_name>
        tags:
          - <tag_name>
          ...
      ...
    descriptions:
      - description: <description_text>
        email: <author_email>
      ...
  ...
output:
  file:
    directory: <output_directory>
```

> Note: You only need to specify `account` if the platform is `SNOWFLAKE`.

See [Output Config](../../common/docs/output.md) for more information on `output`.

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

Here's another example showing how to tag a Snowflake table as `golden`, and the `email` column as `pii`.

```yaml
datasets:
  - id:
      platform: SNOWFLAKE
      account: test_account
      name: database.schema.table1
    tags:
      - golden
    column_tags:
      - column: email
        tags:
          - pii
output:
  file:
    directory: /output
```

The following example shows how to add a description to a Redshift table.

```yaml
datasets:
  - id:
      platform: REDSHIFT
      name: database.schema.table1
    descriptions:
      - description: A fancy description for the table
        email: charlie@test.com
output:
  file:
    directory: /output
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor custom.governance <config_file>
```

Manually verify the output after the run finishes.
