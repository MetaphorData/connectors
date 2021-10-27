# BigQuery Connector

This connector extracts technical metadata from a BigQuery project using [Python Client for Google BigQuery](https://googleapis.dev/python/bigquery/latest/index.html).

## Setup

We recommend creating a dedicated GCP service account with limited permissions for the connector:

1. Go to [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) in Google Cloud Console. Make sure the appropriate project is selected from the project dropdown.
2. Click `Create Service Account` and use the following settings to create a new account:
    - Enter a service account name, e.g. `metaphor-bigquery`.
    - Enter a description, e.g. `Metadata collection for Metaphor app`
    - Click `CREATE AND CONTINUE`.
    - Select `BigQuery Metadata Viewer` as the role and click `CONTINUE`.
    - Click `DONE` to complete the process as there's no need to grant user access to the service account.

Once the service account is created, you need to create a service account key for authentication:

1. Click the newly created account from [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts), then choose `KEYS` tab.
2. Click `ADD KEY` > `Create new key`.
3. Select `JSON` key type and click `CREATE`.
4. Save the generated key file to a secure location.

## Config File

Create a YAML config file based the following template.

### Required Configurations

```yaml
project_id: <bigquery_project_id>
key_path: <path_to_JSON_key_file>
output:
  file:
    path: <path_to_output_file>
```

See [Common Configurations](../common/README.md) for more information on `output`.

### Optional Configurations

You can also optionally specify a list of regular expressions to limit the scan to selected datasets. For example,

```yaml
dataset_filters:
    - my_dataset
    - prefix_.*
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `bigquery` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.bigquery <config_file>
```

Manually verify the output after the run finishes.
