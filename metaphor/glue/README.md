# Glue Connector

This connector extracts technical metadata from AWS Glue using the [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) library.

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
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetDatabase",
                "glue:GetDataBases"
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

You must specify an AWS user credential to access Glue API. You can also specify a role ARN and let the connector assume the role before accessing AWS APIs.

```yaml
aws:
  access_key_id: <aws_access_key_id>
  secret_access_key: <aws_secret_access_key>
  region_name: <aws_region_name>
  assume_role_arn: <aws_role_arn>  # If using IAM role
output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor glue <config_file>
```

Manually verify the output after the run finishes.
