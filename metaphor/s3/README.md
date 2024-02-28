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
path_specs:
  - <PATH_SPEC_1>
  - <PATH_SPEC_2>
verify_ssl: <verify_ssl> 
# Whether or not to verify SSL certificates. By default SSL certificates are verified. You can provide the following            values:
# * False - do not validate SSL certificates. SSL will still be used, but SSL certificates will not be verified.
# * path/to/cert/bundle.pem - A filename of the CA cert bundle to use.  You can specify this argument if you want to use a different CA cert bundle than the one used by botocore.
```

#### Path specifications

This specifies the files / directories to be parse as datasets. Each `path_spec` should follow the below format:

```yaml
path_specs:
  - uri: <URI>
    file_types:
    - <file_type_1>
    - <file_type_2>
    excludes:
    - <excluded_uri_1>
    - <excluded_uri_2>
```

##### URI for files / directories to be ingested

Below are the supported methods to specify which files you want to be ingested as datasets:

###### Ingest a single file as dataset

To map a single file to a dataset, specify your uri as:

```yaml
- uri: "s3://<PATH_TO_YOUR_FILES>
```

Wildcards are supported. For example,

```yaml
- uri: "s3://foo/*/bar/*/*.csv
```

will do what you think it would do.

###### Ingest a directory as a single dataset

You can parse a directory as a single dataset by specifying a `{table}` label in your uri. For example,

```yaml
- uri: "s3://foo/bar/{table}/*/*
```

will parse all directories in `foo/bar` as a dataset. Note that we will pick the most recently created file as the actual table.

###### Ingest a directory as a partitioned dataset

Suppose we have the following file structure:

```
bucket
├── foo
│   └── k1=v1
│       └── k2=v1
│           └── 1.parquet
└── bar
    └── k1=v1
        └── k2=v1
            └── 1.parquet
```

To parse `foo` and `bar` as datasets with partitions created from columns `k1` and `k2`:

```yaml
- uri: "s3://bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

It is also possible to specify partitions without keys. For example, with the following specification:

```yaml
- uri: "s3://bucket/{table}/{partition[0]}/{partition[1]}/*.parquet
```

The connector will consider `k1=v1` and `k2=v1` as two unnamed columns' values.

###### Rules for specifying URI

- The URI must start with `s3://`.
- The bucket name must be specified in the URI.
- Consider providing exact URIs rather than those composed from a bunch of wildcard characters.

##### File types

The following file types are supported:

- "csv"
- "tsv"
- "avro"
- "parquet"
- "json"

All other file types are automatically ignored. If not provided, all these file types will be included.

##### Excluded URIs

The excluded URIs do not support labels.

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Endpoint URL

If you're connecting to S3 compatible storage such as Minio, an endponint URL must be provided:

```yaml
endpoint_url: <endpoint_url>  # The URL for the S3 object storage
```

This is not needed for AWS S3.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `s3` extra.

Run the following command to test the connector locally:

```shell
metaphor s3 <config_file>
```

Manually verify the output after the command finishes.
