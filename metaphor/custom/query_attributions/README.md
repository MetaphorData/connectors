# Custom Query Attribution Connector

This connector attributes queries to specific users. This is particularly useful for queries executed by service accounts on behalf of real users, e.g., BI systems.

## Setup

No special setup is required.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
attributions:
  - platform: <data_platform>
    queries:
      <query_id>: <user_email>
      ...
  ...
```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

### Examples

Here's an example showing how to match user emails to Unity Catalog query logs:

```yaml
attributions:
  - platform: UNITY_CATALOG
    queries:
      query_id_1: joe@test.com
      query_id_2: jane@test.com
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

Run the following command to test the connector locally:

```shell
metaphor custom.query_attributions <config_file>
```

Manually verify the output after the run finishes.
