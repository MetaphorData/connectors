# ThoughtSpot

This connector extracts technical metadata from ThoughtSpot using [ThoughtSpot REST API v2](https://try-everywhere.thoughtspot.cloud/v2/#/everywhere/api/rest/playgroundV2).

## Setup

This connector requires a [ThoughtSpot Everywhere](https://www.thoughtspot.com/everywhere) Edition license to use ThoughtSpot REST API v2. See this documentation [Difference between REST v2 and v2 APIs](https://try-everywhere.thoughtspot.cloud/v2/#/everywhere/documentation/en/?pageid=v1v2-comparison) for more details.

We recommend creating a secret key for the connector.

1. Log into your ThoughtSpot instance.
2. Go to `Develop` tab, click `Customizations` > `Security settings`.
3. Enable trusted authentication.
4. A `secret_key` for trusted authentication is generated.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
user: <user_id>
secret_key: <secret>
base_url: <your_instance_url>  # E.g. https://my1.thoughtspot.cloud

output:
  file:
    directory: <output_directory>
```

### Optional Configurations

We also provide alternative authentication using password.

```yaml
password: <user_password>
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `thought_spot` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
python -m metaphor.thought_spot <config_file>
```

Manually verify the output after the command finishes.
