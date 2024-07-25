# Generate GraphQL client code

## Requirements

- Python >= 3.9
- `ariadne-codegen`

## Usage

```bash
cd metaphor/dbt/cloud/discovery_api
./codegen.sh
```

## Existing files

### `codegen.sh`

Run this script to get the schema from DBT's Apollo server, and generate the corresponding GraphQL client code.

### `queries.graphql`

The queries we will execute from the extractor class.

### `apollo-codegen-config.json`

Copied from [Full Codegen Configuration Example](https://www.apollographql.com/docs/ios/code-generation/codegen-configuration/#full-codegen-configuration-example) on Apollo's site. The only modifications are:

- `endpointURL`
- `outputPath`

### `ariadne-codegen.toml`

Controls the behavior of `ariadne-codegen`.

### `schema.graphql`

The upstream DBT GraphQL schema. This file will be downloaded from upstream whenever `codegen.sh` is run.
