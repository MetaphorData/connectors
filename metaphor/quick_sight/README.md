# QuickSight Connector

This connector extracts technical metadata from AWS QuickSight using the [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) library.

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
                "quicksight:DescribeAnalysis",
                "quicksight:DescribeDashboard",
                "quicksight:DescribeDataSource",
                "quicksight:DescribeDataSet",
                "quicksight:DescribeDataSetRefreshProperties",
                "quicksight:DescribeFolder",
                "quicksight:DescribeUser",
                "quicksight:ListAnalyses",
                "quicksight:ListDashboards",
                "quicksight:ListDataSources",
                "quicksight:ListDataSets",
                "quicksight:ListFolders",
                "quicksight:ListUsers",
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
aws_account_id: <quick_aws_account_id>
```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

Run the following command to test the connector locally:

```shell
metaphor quick_sight <config_file>
```

Manually verify the output after the run finishes.
