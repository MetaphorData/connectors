# Tableau Connector

This connector extracts technical metadata from a Tableau site using [Tableau REST API](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm) & [Tableau Metadata API](https://help.tableau.com/current/api/metadata_api/en-us/index.html). It [requires](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_start.html) Tableau Server 2019.3 or later.

## Setup

We recommend creating a dedicated Tableau user and role with proper permission for the connector to use.

1. Log into your Tableau site as Site Administrator.
2. Go to `Leftside Bar` > `Users`, click `Add Users` > `Add Users by Email`.
3. In the pop-up window, select `Tableau` as authentication method, and provide an email for metaphor connector, then choose the `Server Administrator` (Tableau Server) or `Site Administrator Explorer` (Tableau Online) role. This should generate an email containing a URL link to register the new user. The reason we need administrator role is to read the data source information in order to build the lineage. See https://help.tableau.com/current/server/en-us/dm_perms_assets.htm#permissions-on-metadata for more details.
4. Follow the URL link to create user and password, and login to tableau.

There are two ways to [authenticate against the REST API](https://tableau.github.io/server-client-python/docs/sign-in-out): using access token or user password. The former is recommended by Tableau as a more secure method. If you wish to use that, please also do the step below:

5. Under `User Icon` > `Account Settings` > `Personal Access Tokens`, create a new token with name such as "metaphor-connector", store the generated token value.

### Enabling APIs

Tableau REST API should be enabled by default. You can verify it under `Settings` > `Automatic Access to Metadata about Databases and Tables`.

Tableau Metadata API is disabled by default on Tableau Server. Refer to [these instructions](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_start.html#enable-the-tableau-metadata-api-for-tableau-server) on how to enable it. Note that it may take some time for the initial backfill to complete after enabling the API. You can check the status using the [ingestion status API](https://help.tableau.com/current/server/en-us/dm_tools_backfill.htm).

## Config File

Create a YAML config file based on the following template.

### Required Configurations

If using access token authentication:

```yaml
server_url: <tableau_server_url>  // e.g. https://10ay.online.tableau.com
site_name: <site_name>  // The Tableau Server site you are authenticating with
access_token:
  token_name: <token_name>
  token_value: <token_value>
```

If authenticate via user password:

```yaml
server_url: <tableau_server_url>  // e.g. https://10ay.online.tableau.com
site_name: <site_name>  // The Tableau Server site you are authenticating with
user_password:
  username: <username>
  password: <password>
```

> When connecting to the [Default Site](https://help.tableau.com/current/server/en-us/sites_intro.htm#the-default-site) of a Tableau Server, set `site_name` to an empty string, i.e. `site_name: ''`.

> Remember to prepend the domain name to `username` if you're using Active Directory to authenticate, i.e. `domain_name\username`.

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Alternative Server URL

If the tableau users use a different URL to view content on Tableau than the server URL for fetching metadata, please provide the alternative base URL so the crawler can generate direct links to the assets: 

```yaml
alternative_base_url: <tableau_base_url> // e.g. https://tableau.my_company.com
```

#### Snowflake Account

If one of the data sources is using Snowflake dataset, please provide the Snowflake account as follows,

```yaml
snowflake_account: <account_name>
```

#### BigQuery project ID

If one of the data sources is using BigQuery dataset, please provide the BigQuery project name to project ID mapping if they're not the same.

```yaml
bigquery_project_name_to_id_map:
  project_name1: project_id1
  project_name2: project_id2
```

More information about BigQuery project name and project ID can be found [here](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin) 

#### Preview Images

By default, the connector will request the server to generate a preview image for each view. This can take a significant amount of time for a large number of views. You can disable the feature by setting the following config,

```yaml
disable_preview_image: true
```

#### Excluding Projects

You can specify the project to be included / excluded by the connector. By default the project `Personal Space` is ignored.

To override the default behavior and include `Personal Space` in the connector, use the following configuration:

```yaml
include_personal_space: True
```

To specify the projects to include / exclude, use the following field:

```yaml
projects_filter:
  includes:
    - project_id_1
    - project_id_2
    ...
  excludes:
    - project_id_1
    - project_id_2
    ...
```

To only include specific projects, use `includes` field. To only exclude certain projects, use `excludes` field.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `tableau` extra.

Run the following command to test the connector locally:

```shell
metaphor tableau <config_file>
```

Manually verify the output after the command finishes.
