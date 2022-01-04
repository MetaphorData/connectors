# Tableau Connector

This connector extracts technical metadata from a Tableau site using [Tableau Metadata REST API](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_metadata.htm).

## Setup

We recommend creating a dedicated Tableau user and role with limited permission for the connector to use.

1. Log into your Tableau site as Site Administrator.
2. Go to `Leftside Bar` > `Users`, click `Add Users` > `Add Users by Email`.
3. In the pop-up window, select `Tableau` as authentication method, and provide an email for metaphor connector, then choose the `Viewer` role with read-only permission. This should generate an email containing a URL link to register the new user. 
4. Follow the URL link to create user and password, and login to tableau.

There are two ways to [authenticate against the REST API](https://tableau.github.io/server-client-python/docs/sign-in-out): using access token or user password. The former is recommended by Tableau as a more secure method. If you wish to use that, please also do the step below: 

5. Under `User Icon` > `Account Settings` > `Personal Access Tokens`, create a new token with name such as "metaphor-connector", store the generated token value.

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
output:
  file:
    path: <path_to_output_file>
```

If authenticate via user password:

```yaml
server_url: <tableau_server_url>  // e.g. https://10ay.online.tableau.com
site_name: <site_name>  // The Tableau Server site you are authenticating with
user_password:
  username: <username>
  password: <password>
output:
  file:
    path: <path_to_output_file>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `tableau` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
python -m metaphor.tableau <config_file>
```

Manually verify the output after the command finishes.
