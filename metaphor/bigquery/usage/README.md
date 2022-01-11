# BigQuery Usage Statistics Connector

This connector extract BigQuery usage statistics from a Google cloud project using [Python Client for Cloud Logging](https://googleapis.dev/python/logging/latest/index.html). It calculate usage statistics from BigQuery data read event from [audit log](https://cloud.google.com/logging/docs/audit/services).

## Setup

This connector need an account with permission of `Private Logs Viewers` to view BigQuery log entries.

1. Go to [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) in Google Cloud Console. Make sure the appropriate project is selected from the project dropdown.
2. Click `Create Service Account` and use the following settings to create a new account:
    - Enter a service account name, e.g. `metaphor-bigquery`.
    - Enter a description, e.g. `Metadata collection for Metaphor app`
    - Click `CREATE AND CONTINUE`.
    - Select `Private Logs Viewers` as the role and click `CONTINUE`.
    - Click `DONE` to complete the process as there's no need to grant user access to the service account.

Once the service account is created, you need to create a service account key for authentication:

1. Click the newly created account from [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts), then choose `KEYS` tab.
2. Click `ADD KEY` > `Create new key`.
3. Select `JSON` key type and click `CREATE`.
4. Save the generated key file to a secure location.

## Config File

The config file inherits all the required and optional fields from the general BigQuery connector [Config File](../README.md#config-file). In addition, you can specify the following configurations:

```yaml
# (Optional) Number of days to include in the usage analysis. Default to 30.
lookback_days: <days>

# (Optional) A list of users whose queries will be excluded from the usage calculation 
excluded_usernames:
  - <user_name1>
  - <user_name2>

# (Optional) The number of access logs fetched in a batch, default to 1000, value must be in range 0 - 1000
batch_size: <batch_size>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `bigquery` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```bash
python -m metaphor.bigquery.usage <config_file>
```

Manually verify the output after the run finishes.
