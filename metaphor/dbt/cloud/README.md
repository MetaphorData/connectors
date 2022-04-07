# dbt Cloud Connector

This connector extracts metadata from a [dbt cloud](https://www.getdbt.com/product/what-is-dbt/) project using the [Administrative API](https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/admin-cloud-api).

## Setup

We recommend creating a dedicated [service account token](https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/service-tokens) for the connector with minimally required permissions. This means `Read-only` permissions for Team plan and `Account Viewer` permissions for Enterprise plan.

## Config File

Create a YAML config file based the following template.

### Required Configurations

```yaml
account_id: <dbt_cloud_account_id>
job_id: <dbt_cloud_job_id>
service_token: <service_account_token>
output:
  file:
    directory: <output_directory>
```

You can extract `account_id` & `job_id` from a particular dbt job's URL, which has the format `https://cloud.getdbt.com/#/accounts/<account_id>/projects/<project_id>/jobs/<job_id>/`.

See [Common Configurations](../common/README.md) for more information on `output`.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `dbt` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.dbt.cloud <config_file>
```

Manually verify the output after the run finishes.
