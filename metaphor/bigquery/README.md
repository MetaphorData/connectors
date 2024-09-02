# BigQuery Connector

This connector extracts technical metadata from a BigQuery project using [Python Client for Google BigQuery](https://googleapis.dev/python/bigquery/latest/index.html).

## Setup

We recommend creating a dedicated GCP service account with limited permissions for the connector:

1. Go to [Roles](https://console.cloud.google.com/iam-admin/roles) in Google Cloud Console. Make sure the appropriate project is selected from the project dropdown.
2. Click `Create Role` and use the following settings to create a new custom role for running metadata jobs and fetching logs:
    - Enter a role title, e.g. `Metaphor Job`.
    - Enter a description, e.g. `Metadata collection job role for metaphor app`.
    - Choose role stage `General Availability`.
    - Click `ADD PERMISSIONS` and add: `bigquery.config.get`, `bigquery.jobs.create`, `bigquery.jobs.get`.
    - Click `CREATE` to create the custom role.
3. Go to [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) in Google Cloud Console. Make sure the appropriate project is selected from the project dropdown.
4. Click `Create Service Account` and use the following settings to create a new account:
    - Enter a service account name, e.g., `metaphor-bigquery`.
    - Enter a description, e.g. `Metadata collection for Metaphor app`
    - Click `CREATE AND CONTINUE`.
    - Select `BigQuery Metadata Viewer`, `Private Logs Viewer` and `Metaphor Job` as the roles and click `CONTINUE`.
    - Click `DONE` to complete the process as there's no need to grant user access to the service account.

> Alternatively, if one prefer to avoid predefined roles and use custom role only, here is the complete list of permissions needed:
> - bigquery.config.get
> - bigquery.datasets.get
> - bigquery.datasets.getIamPolicy
> - bigquery.jobs.create
> - bigquery.jobs.get
> - bigquery.models.getMetadata
> - bigquery.models.list
> - bigquery.routines.get
> - bigquery.routines.list
> - bigquery.tables.get
> - bigquery.tables.getIamPolicy
> - bigquery.tables.list
> - logging.logEntries.list
> - logging.logs.list
> - logging.privateLogEntries.list
> - resourcemanager.projects.get


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
project_ids:
  - <bigquery_project_id>
```

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

> Note: `private_key` is a multi-line string and must be specified using YAML's [literal style](https://yaml.org/spec/1.2.2/#812-literal-style), i.e. using a pipe (`|`). You can use this command to extract and format the value from the JSON key:
> ```sh
> cat <JSON_key_file> | jq -r '.private_key' | sed 's/^/    /' 
> ```
> Copy & paste the value into the config file as such:
> ```yaml
> credentials:
>   private_key: |
>     <paste here>
> ```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Filtering

See [Filter Config](../common/docs/filter.md) for more information on the optional `filter` config.

#### Tag Assignment

See [Tag Matcher Config](../common/docs/tag_matcher.md) for more information on the optional `tag_matcher` config.

#### BigQuery Job Project ID

By default, all BigQuery API calls are made using the project ID specified in the credentials. However, sometimes you may wish to run BigQuery jobs using a different project ID, e.g. when querying the DDLs for tables & views. You can do so by specifying the `job_project_id` config as follows,

```yaml
job_project_id: <project_id>
```

#### Concurrency

The max number of concurrent requests to the google cloud API can be configured as follows,

```yaml
max_concurrency: <max_number_of_queries> # Default to 5
```

#### Query Logs

By default, the BigQuery connector will fetch a full day's query logs (AuditMetadata) from yesterday, to be analyzed for additional metadata, such as dataset usage and lineage information. To backfill log data, one can set `lookback_days` to the desired value. To turn off query log fetching, set `lookback_days` to 0.  

```yaml
query_log:
  # (Optional) Number of days of query logs to fetch. Default to 1. If 0, the no query logs will be fetched.
  lookback_days: <days>
  
  # (Optional) A list of users whose queries will be excluded from the log fetching.
  excluded_usernames:
    - <user_name1>
    - <user_name2>

  # (Optional) Exclude queries issued by service accounts. Default to false.
  exclude_service_accounts: <boolean>

  # (Optional) The number of query logs to fetch from BigQuery in one batch. Max 1000, default to 1000.
  fetch_size: <number_of_logs>

  # (Optional) Fetch the full query SQL from job API if it's truncated in the audit metadata log, default True.
  fetch_job_query_if_truncated: <boolean>
```

##### Process Query Config

See [Process Query](../common/docs/process_query.md) for more information on the optional `process_query_config` config.

### Notes

Make sure to use BigQuery project ID when setting the `database` field in the filter configuration. For example:

```yaml
project_id:
  - <bigquery_project_id>
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

Run the following command to test the connector locally:

```shell
metaphor bigquery <config_file>
```

Manually verify the output after the run finishes.
