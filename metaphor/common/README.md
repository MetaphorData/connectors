# Common Configurations

## Output

You can configure the connector to output to files or API.

### Output to Files

File-based output is the preferred way as it enables decoupling between the connector and ingestion pipeline. Add the following fragment to your config file:

```yaml
output:
  file:
    # Location of the output directory.
    # To output to S3 directly, use the format s3://<bucket> or s3://<bucket>/<path>
    directory: <output_directory>

    # (Optional) Maximum number of messages in each output file split. Default to 200.
    bach_size: <messages_per_file>

    # (Optional) IAM role to assume. Default using the current AWS credential.
    assume_role_arn: <iam_role_arn>
```

### Output to API

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
