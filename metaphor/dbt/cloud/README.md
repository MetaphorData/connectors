# dbt Cloud Connector

This connector extracts metadata from a [dbt cloud](https://www.getdbt.com/product/what-is-dbt/) project using the [Administrative API](https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/admin-cloud-api).

## Setup

We recommend creating a dedicated [service account token](https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/service-tokens) for the connector with minimally required permissions. This means `Read-only` permissions for Team plan and `Account Viewer` permissions for Enterprise plan.

## Config File

Create a YAML config file based the following template.

### Required Configurations

```yaml
account_id: <dbt_cloud_account_id>
service_token: <service_account_token>
job_ids:
  - <job_id_1>
  - <job_id_2>
project_ids:
  - <project_id_1>
  - <project_id_2>
output:
  file:
    directory: <output_directory>
```

You can extract `account_id` & `job_ids` from particular dbt job URLs, which have the format `https://cloud.getdbt.com/#/accounts/<account_id>/projects/<project_id>/jobs/<job_id>/`.

It is also possible to specify the IDs for the projects to extract. The connector will extract the last successful run of each of the project's jobs.

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations

#### Base URL

If you're using dbt [Single Tenancy](https://docs.getdbt.com/docs/cloud/about-cloud/tenancy#single-tenant), you'll also need to specify a different base URL:

```yaml
base_url: https://cloud.<tenant>.getdbt.com
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `dbt` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
metaphor dbt.cloud <config_file>
```

Manually verify the output after the run finishes.
