# Output Config

You can configure the connector to output to files or API.

## Output to Files

File-based output is the preferred way as it enables decoupling between the connector and ingestion pipeline. Add the following fragment to your config file:

```yaml
output:
  file:
    # Location of the output directory.
    # To output to S3 directly, use the format s3://<bucket> or s3://<bucket>/<path>
    directory: <output_directory>

    # (Optional) Maximum number of messages in each output file split. Default to 200.
    batch_size_count: <messages_per_file>

    # (Optional) Maximum size for each output file split. Default to 100 MB.
    batch_size_size: <size_in_bytes>

    # (Optional) IAM role to assume. Default using the current AWS credential.
    assume_role_arn: <iam_role_arn>
```

### Write to S3

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

## Output to API

You can choose to output the results to an API using the following config:

```yaml
output:
  api:
    url: <api_endpoint_url>
    api_key: <api_key>

    # (Optional) Maximum number of messages to send per call. Default to 20.
    batch_size: <messages_per_api_call>

    # (Optional) Timeout for the API call in seconds. Default to 30.
    timeout: <api_timeout>
```
