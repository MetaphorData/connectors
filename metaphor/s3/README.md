# S3 Connector

This connector extracts technical metadata from a S3 compatible object storage.

## Setup

To locally setup a S3 compatible storage, run the following command:

```shell
docker-compose -f metaphor/s3/docker-compose.yml up -d
```

This sets up a Minio service, with its data prepopulated with the fake data defined in oure unit test folder.

### Required Configurations

You must specify an AWS user credential to access S3 API. You can also specify a role ARN and let the connector assume the role before accessing AWS APIs.

```yaml
aws:
  access_key_id: <aws_access_key_id>
  secret_access_key: <aws_secret_access_key>
  region_name: <aws_region_name>
  assume_role_arn: <aws_role_arn>  # If using IAM role
  session_token: <aws_session_token>  # If using session token
  profile_name: <aws_profile_name>  # If using AWS profile
endpoint_url: <endpoint_url>  # The URL for the S3 object storage
path_specs:
  - <PATH_SPEC_1>
  - <PATH_SPEC_2>
verify_ssl: <verify_ssl> 
# Whether or not to verify SSL certificates. By default SSL certificates are verified. You can provide the following            values:
# * False - do not validate SSL certificates. SSL will still be used, but SSL certificates will not be verified.
# * path/to/cert/bundle.pem - A filename of the CA cert bundle to use.  You can specify this argument if you want to use a different CA cert bundle than the one used by botocore.

output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

#### Path specifications

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `s3` extra.

To test the connector locally, change the config file to output to a local path and run the following command.

```shell
metaphor s3 <config_file>
```

Manually verify the output after the command finishes.
