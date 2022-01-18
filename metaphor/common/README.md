# Common Configurations

## Output

You can configure the connector to output to files or API.

### Output to Files

File-based output is the preferred way as it enables decoupling between the connector and ingestion pipeline. Add the following fragment to your config file:

```yaml
output:
  file:
    # Location of the output. Must have a ".json" extension.
    # To output to S3 directly, use the format s3://<bucket>/<object>
    path: <output_path>

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


## (Optional) Filter

By default, the connector will extract metadata from all tables/views in all schemas and databases. You can optionally limit it by specifying the `filter` option. For example, the following config will only include tables/views from database `db1` and `db2`:

```yaml
filter:
  includes:
    db1:    
    db2:
```

You can also exclude only specific databases, schemas, or tables/views. For example, the following will include all tables/views except `db1.*`, `db2.schema1.*`, and `db3.schema1.table1`:

```yaml
filter:
  excludes:
    db1:  
    db2:
      schema1:
    db3:
      schema1:
        - table1
```

Note that when there's an overlap between `includes` and `excludes`, the latter will always take precedence. For instance, the following config will include all tables under `db1`, except `db1.schema1.table1` and `db1.schema1.table2`:

```yaml
filter:
  includes:
    db1:
  excludes:
    db1:
      schema1:  
        - table1
        - table2

```
