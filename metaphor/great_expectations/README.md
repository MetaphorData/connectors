# Great Expectations Connector

This connector extracts technical metadata from Great Expectations using [GX Core](https://greatexpectations.io/gx-core).

## Setup

This connector runs by parsing existing Great Expectations run artifacts. To use this connector, make sure the execution context has been persisted. For example:

```python
import great_expectations as gx

ctx = gx.get_context(mode="file", project_root_dir="SOME_DIR") # This works, the artifacts are persisted to `SOME_DIR`, the connector will parse it to get the validation results.

# ctx = gx.get_context() # XXX This does not work, it creates a context only in memory and nothing is persisted
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

```yaml
project_root_dir: <PROJECT_ROOT_DIR> # The project root directory. This is the directory that contains your Great Expectations artifacts, i.e. where the `gx` directory lives.
```

### Optional Configurations

```yaml
snowflake_account: <SNOWFLAKE_ACCOUNT> # The Snowflake account to use if the Great Expectations run was targeted at Snowflake.
```

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv).

Run the following command to test the connector locally:

```shell
metaphor great_expectations <config_file>
```

Manually verify the output after the command finishes.
