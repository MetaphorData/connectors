# Custom data quality Connector

This connector attaches custom data quality metadata to tables.

## Setup

No special setup is required.

## Config File

Create a YAML config file based on the following template.

```yaml
datasets:
  - id:
      platform: [SNOWFLAKE|BIGQUERY|REDSHIFT|...]
      name: <dataset_name>
      account: <snowflake_account>  # only for Snowflake
    data_quality:
      provider: [SODA|LIGHTUP|BIGEYE]  # optional
      url: <summary_page_url>  # optional
      monitors:
        - title: <monitor_name>
          description: <monitor_description>  # optional
          url: <monitor_url>  # optional
          owner: <owner_email>  # optional
          status: [PASSED|WARNING|ERROR]
          severity: [LOW|MEDIUM|HIGH]  # optional
          last_run: <ISO 8901 date + time>  # optional
          value: <monitor_value>  # optional
          targets:
            - dataset: <dataset_id>  # optional
              column: <column_name>  # optional
              ...
        ...
  ...
output:
  file:
    directory: <output_directory>
```

> Note: You only need to specify `account` if the platform is `SNOWFLAKE`.

See [Output Config](../../common/docs/output.md) for more information on `output`.

### Examples

Here's an example showing how to attach data quality metadata that contains two monitors:

```yaml
datasets:
  - id:
      platform: BIGQUERY
      name: database.schema.table1
    data_quality:
      monitors:
        - title: check_max_value
          status: PASSED
          last_run: 2022-10-16T07:00:40
          targets:
            - column: col1 
        - title: check_not_null
          status: ERROR
          last_run: 2022-10-16T07:00:40
          targets:
            - column: col2
output:
  file:
    directory: /output
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor custom.data_quality <config_file>
```

Manually verify the output after the run finishes.
