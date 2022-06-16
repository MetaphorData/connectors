# BigQuery Query Connector

This connector extracts BigQuery query history from a Google cloud project using [Python Client for Cloud Logging](https://googleapis.dev/python/logging/latest/index.html). It parses the [jobChange events](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#BigQueryAuditMetadata.JobChange) from the [BigQuery audit logs](https://cloud.google.com/bigquery/docs/reference/auditlogs).

## Setup

Create a [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) based on the [Setup](../README.md#Setup) guide for the general BigQuery connector. You'll need to grant additional permissions to the account to view the [audit log](https://cloud.google.com/logging/docs/audit/services). You can add the `Private Logs Viewers` role to your service account or add the following permissions to the custom IAM role your service account bind with:

```text
logging.logEntries.list
logging.logs.list
logging.logServiceIndexes.list
logging.logServices.list
logging.privateLogEntries.list
resourcemanager.projects.get
```

See [Access Control](https://cloud.google.com/logging/docs/access-control#console_permissions) for more information.

## Config File

The config file inherits all the required and optional fields from the general BigQuery connector [Config File](../README.md#config-file). In addition, you can specify the following optional configurations:

```yaml
# (Optional) Number of days of logs to extract for lineage analysis. Default to 30.
lookback_days: <days>

# (Optional) The number of access logs fetched in a batch, default to 1000, value must be in range 0 - 1000
batch_size: <batch_size>

# (Optional) Maximum number of recent queries to extract per table. Default to 100.
max_queries_per_table: <number>

# (Optional) Exclude queries issued by service accounts. Default to true.
exclude_service_accounts: <boolean>

# (Optional) Exclude queries issued by these users.
excluded_usernames:
  - user1
  - user2
```

## Testing

Follow the [Installation](../../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `bigquery` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```bash
python -m metaphor.bigquery.query <config_file>
```

Manually verify the output after the run finishes.
