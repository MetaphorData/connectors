# OpenAPI Connector

This connector extracts APIs from an OpenAPI Specification JSON.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
base_url: <url>  # BaseUrl for endpoints in OAS
openapi_json_path: <path or url>  # URL or path of OAS
```

### Optional Configurations

If accessing the OAS JSON requires authentication, please include an optional auth configuration.

```yaml
auth:
  basic_auth:
    user: <user>
    password: <password>
```

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include the `openapi` or `all` extra.

Run the following command to test the connector locally:

```shell
metaphor openapi <config_file>
```

Manually verify the output after the run finishes.
