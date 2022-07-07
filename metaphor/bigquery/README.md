# BigQuery Connector

This connector extracts technical metadata from a BigQuery project using [Python Client for Google BigQuery](https://googleapis.dev/python/bigquery/latest/index.html).

## Setup

We recommend creating a dedicated GCP service account with limited permissions for the connector:

1. Go to [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) in Google Cloud Console. Make sure the appropriate project is selected from the project dropdown.
2. Click `Create Service Account` and use the following settings to create a new account:
    - Enter a service account name, e.g., `metaphor-bigquery`.
    - Enter a description, e.g. `Metadata collection for Metaphor app`
    - Click `CREATE AND CONTINUE`.
    - Select `BigQuery Metadata Viewer` as the role and click `CONTINUE`.
    - Click `DONE` to complete the process as there's no need to grant user access to the service account.

Once the service account is created, you need to create a service account key for authentication:

1. Click the newly created account from [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts), then choose `KEYS` tab.
2. Click `ADD KEY` > `Create new key`.
3. Select `JSON` key type and click `CREATE`.
4. Save the generated key file to a secure location.

Your key file may look like:

```json
{
  "type": "service_account",
  "project_id": "<project_id>",
  "private_key_id": "<key_id>",
  "private_key": "-----BEGIN PRIVATE KEY-----\n<key>\n-----END PRIVATE KEY-----",
  "client_email": "<email>@<project_id>.iam.gserviceaccount.com",
  "client_id": "<client_id>",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/<client_cert_url>"
}
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
project_id: <bigquery_project_id>
output:
  file:
    directory: <output_directory>
```

See [Common Configurations](../common/README.md) for more information on `output`.

To connect to BigQuery, either the keyfile path or credentials from the JSON keyfile must be set in the config as following:

```yaml
key_path: <path_to_JSON_key_file>
```

or

```yaml
credentials:
  project_id: <project_id>
  private_key_id: <private_key_id>
  private_key: <private_key_value>
  client_email: <client_email>
  client_id: <client_id>
```

### Optional Configurations

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Concurrency

The max number of concurrent requests to the google cloud API can be configured as follows,

```yaml
max_concurrency: <max_number_of_queries> # Default to 10
```

### Notes

Make sure to use BigQuery project ID when setting the `database` field in the filter configuration. For example:

```yaml
project_id: <bigquery_project_id>
filter:
  includes:
    <bigquery_project_id>:
      schema1:
      schema2:
  excludes:
    <bigquery_project_id>:
      schema1:
        - table1
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `bigquery` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.bigquery <config_file>
```

Manually verify the output after the run finishes.
