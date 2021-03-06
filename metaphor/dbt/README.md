# dbt Connector

This connector extracts technical metadata from a [dbt project](https://docs.getdbt.com/docs/building-a-dbt-project/projects) by parsing the `manifest.json` & `catalog.json` files generated by `dbt docs generate` or `dbt run`command.

## Setup

There is no special setup needed to run the connector as a CLI. However, we recommend running it as part of your dbt project's CI/CD workflow so that the metadata is refreshed automatically with each commit. Please refer to [Metaphor dbt GitHub Action](https://github.com/MetaphorData/dbt-action) for more details.

The remaining sections are for those who intend to run the connector manually as a CLI or to integrate it into a different CI/CD environment.

## Config File

Create a YAML config file based the following template.

### Required Configurations

```yaml
manifest: <path_to_manifest_json>
catalog: <path_to_catalog_json>
output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations

#### Snowflake Account

If the dbt project is using Snowflake, please provide the Snowflake account as follows,

```yaml
account: <snowflake_account_name>
```

#### URLs

You can optionally provide the `docs_base_url` (base URL serving the dbt generated docs) and `project_source_url` (source code URL pointing to the project root directory). Those will help us to generate links to a model's docs and source code.

```yaml
docs_base_url: <docs_base_url>
project_source_url: <project_dir_source_code_url>
```

#### Ownership

You can optionally specify the ownership for the materialized table or view using the [meta config](https://docs.getdbt.com/reference/resource-configs/meta) of a dbt model. For example:

```yaml
models:
  - name: users
    config:
      meta:
        owner: joe@test.com
```

To map `owner` to the corresponding ownership type defined in Metaphor, add the following to the config file:

```yaml
meta_ownerships:
  - meta_key: owner
    ownership_type: Data Steward
```

If the `owner` field contains ony the user name (e.g. `joe` instead of `joe@test.com`), you can specify the common email domain using the `email_domain` config:

```yaml
meta_ownerships:
  - meta_key: owner
    ownership_type: Data Steward
    email_domain: test.com
```

#### Tags

Similar to [Ownership](#ownership), you can optionally specify certain attributes in meta. For example:

```yaml
models:
  - name: users
    config:
      meta:
        pii: true
```

To map `pii` to the corresponding tag type defined in Metaphor, add the following to the config file:

```yaml
meta_tags:
  - meta_key: pii
    tag_type: HAS_PII
```

By default, only attributes with a value of `true` will be mapped. You can optionally specify a regex in `meta_value_matcher` to match other types of values. For example:

```yaml
models:
  - name: users
    config:
      meta:
        team: sales
```

Use the following config to map it to the "SALES" tag on Metaphor:

```yaml
meta_tags:
  - meta_key: team
    meta_value_matcher: sales
    tag_type: SALES
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `dbt` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```
python -m metaphor.dbt <config_file>
```

Manually verify the output after the run finishes.
