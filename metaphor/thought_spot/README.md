# ThoughtSpot

This connector extracts technical metadata from ThoughtSpot using [ThoughtSpot REST API v2](https://try-everywhere.thoughtspot.cloud/v2/#/everywhere/api/rest/playgroundV2).

## Setup

This connector requires a [ThoughtSpot Everywhere](https://www.thoughtspot.com/everywhere) Edition license to use ThoughtSpot REST API v2. See [REST v1 and REST v2 API comparison](https://try-everywhere.thoughtspot.cloud/v2/#/everywhere/documentation/en/?pageid=v1v2-comparison) for more details.

We recommend creating a secret key for the connector.

1. Log into your ThoughtSpot instance as an administrator (with the "Can administer ThoughtSpot" [privilege](https://docs.thoughtspot.com/software/latest/groups-privileges)).
2. Go to `Develop` tab, click `Security settings` under **Customizations**.
3. Enable `Trusted authentication`.
4. A `secret_key` for trusted authentication is generated.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
base_url: <your_instance_url>  # e.g. https://my1.thoughtspot.cloud
user: <user_id>

# If using secret key to authenticate
secret_key: <secret>

# If using password to authenticate
password: <password>

output:
  file:
    directory: <output_directory>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `thought_spot` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor thought_spot <config_file>
```

Manually verify the output after the command finishes.
