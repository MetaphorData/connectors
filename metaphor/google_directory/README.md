# Google Directory Connector

This connector extracts user information from a Google Workspace using [Directory API](https://developers.google.com/admin-sdk/directory).

## Setup

1. Create a [new GCP project](https://support.google.com/googleapi/answer/6251787?hl=en#zippy=%2Ccreate-a-project). Select the project from the project dropdown list once it's created (may takes a few seconds).

2. Go to [Oauth consent screen](https://console.cloud.google.com/apis/credentials/consent) (Main menu > `API & Services` > `Oauth consent screen`) and follow these steps to setup a consent screen,
    - Choose Internal then click `CREATE`.
    - Fill out only the required fields then click `SAVE AND CONTINUE`. Since the consent screen is only expected to be used by you to generate refresh token, the values here are not very important.
    - Click `SAVE AND CONTINUE` in the Scopes step.
    - Click `BACK TO DASHBOARD` in the Summary step.

2. Go to [Credentials](https://console.cloud.google.com/apis/credentials) (main menu > `API & Services` > `Credentials`).

3. Follow the steps below to create a new OAuth client ID
    - Click `CREATE CREDENTIALS` > `OAuth client ID`.
    - Choose `Desktop app` as Application type and provide a sensible Name. Click `CREATE`.
    - Click `OK` to dismiss the dialog, then click the download icon for the newly created client ID and save the JSON file locally.

4. Go to [Admin SDK API](https://console.cloud.google.com/apis/library/admin.googleapis.com) (main menu > `API & Services` > `Libray` > search for `Admin SDK API`) and click `ENABLE`.

## Generate OAuth2 Credential File

The file downloaded in the previous section contains the OAuth2 client ID & secret. We need to use them to exchange for a non-expiring refresh token. This is a one-off process that requires a manual consent from a user with the appropriate permissions, generally the Google Workspace Admin.

1. Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `google_directory` extra.

2. Run the following command to generate the token file

```text
python -m metaphor.google_directory.authenticate <path_to_downloaded_json> <output_path_for_token_file>
``` 

3. Login in the browser using the appropriate account then click `Allow`.

## Config File

Create a YAML config file based the following template.

```yaml
token_file: <path_to_token_file>
output:
  file:
    directory: <output_directory>
```

See [Common Configurations](../common/README.md) for more information on `output`.

## Testing

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.google_directory <config_file>
```

Manually verify the output after the run finishes.
