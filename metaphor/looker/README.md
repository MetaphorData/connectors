# Looker Connector

This connector extracts technical metadata from a LookML project and using [Looker API](https://docs.looker.com/reference/api-and-integration/api-reference/v3.1).

## Setup

We recommend creating a dedicated Looker user and role with limited permission for the connector to use.

1. Log into your Looker instance.
2. Go to `Admin` > `Roles`, click `New Permission Set`.
3. Name the permission set `Metaphor` and select the following permissions to create the role:
    - `access_data`
        - `see_lookml_dashboards`
        - `see_looks`
            - `see_user_dashboards`
            - `explore`
        - `see_sql`
        - `see_queries`
        - `see_logs`
        - `see_users`
        - `see_pdts`
        - `see_system_activity`
4. Click the `New Role` button to create a new role using the following:
    - **Name**: `Metaphor`
    - **Permission Set**: select `Metaphor`
    - **Model Set**: select `All`
    - Leave **Groups** and **Users** unselected
5. Go to `Admin` > `Users`, click `Add Users` to create a new user using the following:
    - **Email**: You can use any email address as the user is only used for API access.
    - **Send Setup Email**: Unselect
    - **Roles**: Select `Metaphor`
    - **Groups**: Unselect

## API Authentication

Now the user has been created, we need to generate the client ID & secret for Looker API access:

1. Go to `Admin` > `Users`, click the new user's `Edit` button.
2. Click the `Edit Keys` button under **API3 Keys**.
3. Click `New API3 Key`, then note down the **Client ID** and **Client Secret** for the config file.

## GitHub Action

The recommended way to run the connector in production is to integrate it as part of the CI/CD pipeline. Please refer to [Metaphor Looker GitHub Action](https://github.com/MetaphorData/looker-action) for more details.

The remaining sections are for those who intend to run the connector manually as a CLI or to integrate it into a different CI/CD environment. 

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
base_url: <looker_base_url>
client_id: <client_id>
client_secret: <client_secret>
lookml_dir: <path_to_lookml_project>
connections:
  <name>:
    database: <database_name>
    default_schema: <schema_name>
    account: <snowflake_account>
    platform: SNOWFLAKE
output:
  file:
    directory: <output_directory>
```

Note that `connections` is a mapping of database connection names to connection settings. You can find these settings under `Admin` > `Connections`. For now, the only platform supported is `SNOWFLAKE` with `account` set to the matching [Snowflake Account Identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).

See [Common Configurations](../common/README.md) for more information on `output`.

### Optional Configurations

To add a "View LookML" link to Looker explore & views on Metaphor you need to specify a base URL for the Looker project. This can be either a URL to the GitHub repository (`https://github.com/<account>/<repo>`) or Looker IDE (`https://<account>.cloud.looker.com/projects/<project>/files/`).

```yaml
projectSourceUrl: <looker_project_url>
```

You can also disable SSL verify and change the request timeout if needed, e.g.

```yaml
verify_ssl: false 
timeout: 30  # default 120 seconds
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `looker` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
python -m metaphor.looker <config_file>
```

Manually verify the output after the command finishes.
