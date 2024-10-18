# Athena Connector

This connector extracts technical metadata and query history from AWS Athena using the [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) library.

## Setup

We recommend creating a dedicated AWS IAM user for the crawler with limited permissions based on the following IAM policy:

``` json
{
    "Version": "2012-10-17",
    "Statement":
    [
        {
            "Effect": "Allow",
            "Action":
            [
                "athena:ListDataCatalogs",
                "athena:ListDatabases",
                "athena:ListTableMetadata",
                "athena:ListQueryExecutions",
                "athena:BatchGetQueryExecution",
                "glue:GetDatabases",
                "glue:GetTables"
            ],
            "Resource":
            [
                "*"
            ]
        }
    ]
}
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

You must specify an AWS user credential to access QuickSight API. You can also specify a role ARN and let the connector assume the role before accessing AWS APIs.

```yaml
aws:
  access_key_id: <aws_access_key_id>
  secret_access_key: <aws_secret_access_key>
  region_name: <aws_region_name>
  assume_role_arn: <aws_role_arn>  # If using IAM role
```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Query Log Extraction Configurations

The Athena connector will enable query log extraction by default

```yaml
query_log:
  # (Optional) Number of days of query logs to fetch. Default to 1. If 0, the no query logs will be fetched.
  lookback_days: <days>

  # (Optional) WorkGroups to collect query history, default to []. If not specify, collect from the primary workgroup
  work_groups:
    - workgroup_1
    - ...

  # (Optional)
  process_query: <process_query_config>
```

##### Process Query Config

See [Process Query](../common/docs/process_query.md) for more information on the optional `process_query_config` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

Run the following command to test the connector locally:

```shell
metaphor athena <config_file>
```

Manually verify the output after the run finishes.
