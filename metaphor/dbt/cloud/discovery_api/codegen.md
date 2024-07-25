# Generate GraphQL client

This is done by `ariadne-codegen`. Python version has to be >= 3.9.

## Usage

1. Navigate to `{GIT_ROOT}/metaphor/dbt/cloud/discovery_api`
2. Run `poetry run ariadne-codegen --config ./ariadne-codegen.toml`

## Components

### `schema.graphql`

Yanked from https://metadata.cloud.getdbt.com/graphql.

### `queries.graphql`

These are the actual queries.

### `generated`

The generated GraphQL client.
