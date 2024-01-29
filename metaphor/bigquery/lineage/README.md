# BigQuery Lineage Connector

This connector extracts BigQuery lineage information from a Google cloud project using [Python Client for Cloud Logging](https://googleapis.dev/python/logging/latest/index.html). It computes dataset lineage from [jobChange event](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#BigQueryAuditMetadata.JobChange) from the [BigQuery audit logs](https://cloud.google.com/bigquery/docs/reference/auditlogs).

## Setup

Create a [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) based on the [Setup](../README.md#Setup) guide for the general BigQuery connector.

See [Access Control](https://cloud.google.com/logging/docs/access-control#console_permissions) for more information.

## Config File

The config file inherits all the required and optional fields from the general BigQuery connector [Config File](../README.md#config-file). In addition, you can specify the following configurations:

```yaml
# (Optional) Whether to enable parsing view definition to build view lineage, default True
enable_view_lineage: <boolean>

# (Optional) Whether to enable parsing audit log to find table lineage information, default True
enable_lineage_from_log: <boolean>

# (Optional) Whether to include self-referencing loops in lineage, default True
include_self_lineage: <boolean>

# (Optional) Number of days of logs to extract for lineage analysis. Default to 7.
lookback_days: <days>

# (Optional) The number of access logs fetched in a batch, default to 1000, value must be in range 0 - 1000
batch_size: <batch_size>
```

## Testing

Follow the [Installation](../../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `bigquery` extra.

Run the following command to test the connector locally:

```bash
metaphor bigquery.lineage <config_file>
```

Manually verify the output after the run finishes.
