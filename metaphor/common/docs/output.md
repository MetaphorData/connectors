# Output Config

You can configure the connector to output to files or API.

## Output to Local Files

File-based output is the preferred way as it enables decoupling between the connector and ingestion pipeline. Add the following fragment to your config file:

```yaml
output:
  file:
    # Location of the output directory, e.g. /tmp/metaphor
    directory: <output_directory>

    # (Optional) Maximum number of messages in each output file split. Default to 200.
    batch_size_count: <messages_per_file>

    # (Optional) Maximum size for each output file split. Default to 100 MB.
    batch_size_size: <size_in_bytes>

    # (Optional) IAM role to assume. Default using the current AWS credential.
    assume_role_arn: <iam_role_arn>
```

## Output to S3

To write the output to a S3 bucket, you must also add the AWS region & credentials to the config:

```yaml
output:
  file:
    directory: s3://<bucket>/<path>

    s3_auth_config:
      aws_access_key_id: <AWS_ACCESS_KEY_ID>
      aws_secret_access_key: <AWS_SECRET_ACCESS_KEY>
      region_name: <AWS_REGION>
```
