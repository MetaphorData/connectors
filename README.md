<a href="https://metaphor.io"><img src="https://github.com/MetaphorData/connectors/raw/main/logo.png" width="300" /></a>

# Metaphor Connectors

[![Codecov](https://img.shields.io/codecov/c/github/MetaphorData/connectors)](https://app.codecov.io/gh/MetaphorData/connectors/tree/main)
[![CodeQL](https://github.com/MetaphorData/connectors/workflows/CodeQL/badge.svg)](https://github.com/MetaphorData/connectors/actions/workflows/codeql-analysis.yml)
[![PyPI Version](https://img.shields.io/pypi/v/metaphor-connectors)](https://pypi.org/project/metaphor-connectors/)
![Python version 3.8+](https://img.shields.io/badge/python-3.8%2B-blue)
![PyPI Downloads](https://img.shields.io/pypi/dm/metaphor-connectors)
[![Docker Pulls](https://img.shields.io/docker/pulls/metaphordata/connectors)](https://hub.docker.com/r/metaphordata/connectors)
[![License](https://img.shields.io/github/license/MetaphorData/connectors)](https://github.com/MetaphorData/connectors/blob/master/LICENSE)

This repository contains a collection of Python-based "connectors" that extract metadata from various sources to ingest into the [Metaphor](https://metaphor.io) platform.

## Installation

This package requires Python 3.8+ installed. You can verify the version on your system by running the following command,

```shell
python -V  # or python3 on some systems
```

Once verified, you can install the package using [pip](https://docs.python.org/3/installing/index.html),

```shell
pip install "metaphor-connectors[all]"  # or pip3 on some systems
```

This will install all the connectors and required dependencies. You can also choose to install only a subset of the dependencies by installing the specific [extra](https://packaging.python.org/tutorials/installing-packages/#installing-setuptools-extras), e.g.

```shell
pip install "metaphor-connectors[snowflake]"
```

Similarly, you can also install the package using `requirements.txt` or `pyproject.toml`.

## Docker

We automatically push a [docker image](https://hub.docker.com/r/metaphordata/connectors) to Docker Hub as part of the CI/CD. See [this page](./docs/docker.md) for more details.

## GitHub Action

You can also run the connectors in your CI/CD pipeline using the [Metaphor Connectors](https://github.com/marketplace/actions/metaphor-connectors-github-action) GitHub Action.

## Connectors

Each connector is placed under its own directory under [metaphor](./metaphor) and extends the `metaphor.common.BaseExtractor` class.

| Connector Name                                                    | Metadata                                 |
|-------------------------------------------------------------------|------------------------------------------|  
| [azure_data_factory](metaphor/azure_data_factory/)                | Lineage, Pipeline                        |
| [bigquery](metaphor/bigquery/)                                    | Schema, description, statistics, queries |
| [bigquery.lineage](metaphor/bigquery/lineage/)                    | Lineage                                  |
| [bigquery.profile](metaphor/bigquery/profile/)                    | Data profile                             |
| [confluence](metaphor/confluence/)                                | Document embeddings                      |
| [custom.data_quality](metaphor/custom/data_quality/)              | Data quality                             |
| [custom.governance](metaphor/custom/governance/)                  | Ownership, tags, description             |
| [custom.lineage](metaphor/custom/lineage/)                        | Lineage                                  |
| [custom.metadata](metaphor/custom/metadata/)                      | Custom metadata                          |
| [custom.query_attributions](metaphor/custom/query_attributions/)  | Query attritutions                       |
| [datahub](metaphor/datahub/)                                      | Description, tag, ownership              |
| [dbt](metaphor/dbt/)                                              | dbt model, test, lineage                 |
| [dbt.cloud](metaphor/dbt/cloud/)                                  | dbt model, test, lineage                 |
| [fivetran](metaphor/fivetran/)                                    | Lineage, Pipeline                        |
| [glue](metaphor/glue/)                                            | Schema, description                      |
| [looker](metaphor/looker/)                                        | Looker view, explore, dashboard, lineage |
| [kafka](metaphor/kafka/)                                          | Schema, description                      |
| [metabase](metaphor/metabase/)                                    | Dashboard, lineage                       |
| [monte_carlo](metaphor/monte_carlo/)                              | Data monitor                             |
| [mssql](metaphor/mssql/)                                          | Schema                                   |
| [mysql](metaphor/mysql/)                                          | Schema, description                      |
| [notion](metaphor/notion/)                                        | Document embeddings                      |
| [postgresql](metaphor/postgresql/)                                | Schema, description, statistics          |
| [postgresql.profile](metaphor/postgresql/profile/)                | Data profile                             |
| [postgresql.usage](metaphor/postgresql/usage/)                    | Usage                                    |
| [power_bi](metaphor/power_bi/)                                    | Dashboard, lineage                       |
| [redshift](metaphor/redshift/)                                    | Schema, description, statistics, queries |
| [redshift.profile](metaphor/redshift/profile/)                    | Data profile                             |
| [sharepoint](metaphor/sharepoint/)                                | Document embeddings                      |
| [snowflake](metaphor/snowflake/)                                  | Schema, description, statistics, queries |
| [snowflake.lineage](metaphor/snowflake/lineage/)                  | Lineage                                  |
| [snowflake.profile](metaphor/snowflake/profile/)                  | Data profile                             |
| [static_web](metaphor/static_web/)                                | Document embeddings                      |
| [synapse](metaphor/synapse/)                                      | Schema, queries                          |
| [tableau](metaphor/tableau/)                                      | Dashboard, lineage                       |
| [thought_spot](metaphor/thought_spot/)                            | Dashboard, lineage                       |
| [trino](metaphor/trino/)                                          | Schema, description, queries             |
| [unity_catalog](metaphor/unity_catalog/)                          | Schema, description                      |
| [unity_catalog.profile](metaphor/unity_catalog/profile/)          | Data profile, statistics                 |

## Development

See [Development Environment](docs/develop.md) for more instructions on how to set up your local development environment.

## Custom Connectors

See [Adding a Custom Connector](docs/custom.md) for instructions and a full example of creating your custom connectors.
