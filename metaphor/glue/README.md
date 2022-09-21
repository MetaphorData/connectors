# Glue Connector

This connector extracts technical metadata from AWS Glue using the [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) library.

## Setup

Prepare an AWS credential with the following IAM permissions.

``` text
effect = "Allow"
actions = [
  "glue:GetTable",
  "glue:GetTables",
  "glue:GetDatabase",
  "glue:GetDataBases"
]
resources = [
  "*" // or specific resources
]
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

If using user password authentication:

```yaml
aws:
  access_key_id: <aws_access_key_id>
  secret_access_key: <aws_secret_access_key>
  region_name: <aws_region_name>
output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations

You can also specify a role ARN with proper policy and let the connector assume the role before accessing AWS APIs.

```yaml
aws:
  assume_role_arn: <aws_role_arn>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor glue <config_file>
```

Manually verify the output after the run finishes.
