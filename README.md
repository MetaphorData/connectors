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

We automatically push a [docker image](https://hub.docker.com/r/metaphordata/connectors) to Docker Hub as par of the CI/CD. See [this page](./docs/docker.md) for more details.

## Connectors

Each connector is placed under its own directory under [metaphor](./metaphor) and extends the `metaphor.common.BaseExtractor` class.

| Connector Name                                                | Metadata                                 |
|---------------------------------------------------------------|------------------------------------------|  
| [airflow_plugin](metaphor/airflow_plugin/README.md)           | Lineage                                  |
| [azure_data_factory](metaphor/azure_data_factory/README.md)   | Lineage, Pipeline                        |
| [bigquery](metaphor/bigquery/README.md)                       | Schema, description, statistics, queries |
| [bigquery.lineage](metaphor/bigquery/lineage/README.md)       | Lineage                                  |
| [bigquery.profile](metaphor/bigquery/profile/README.md)       | Data profile                             |
| [dbt](metaphor/dbt/README.md)                                 | dbt model, test, lineage                 |
| [dbt.cloud](metaphor/dbt/cloud/README.md)                     | dbt model, test, lineage                 |
| [fivetran](metaphor/fivetran/README.md)                       | Lineage, Pipeline                        |
| [glue](metaphor/glue/README.md)                               | Schema, description                      |
| [looker](metaphor/looker/README.md)                           | Looker view, explore, dashboard, lineage |
| [custom.data_quality](metaphor/custom/data_quality/README.md) | Data quality                             |
| [custom.governance](metaphor/custom/governance/README.md)     | Ownership, tags, description             |
| [custom.lineage](metaphor/custom/lineage/README.md)           | Lineage                                  |
| [custom.metadata](metaphor/custom/metadata/README.md)         | Custom metadata                          |
| [metabase](metaphor/metabase/README.md)                       | Dashboard, lineage                       |
| [monte_carlo](metaphor/monte_carlo/README.md)                 | Data monitor                             |
| [mssql](metaphor/mssql/README.md)                             | Schema                                   |
| [mysql](metaphor/mysql/README.md)                             | Schema, description                      |
| [postgresql](metaphor/postgresql/README.md)                   | Schema, description, statistics          |
| [postgresql.profile](metaphor/postgresql/profile/README.md)   | Data profile                             |
| [postgresql.usage](metaphor/postgresql/usage/README.md)       | Usage                                    |
| [power_bi](metaphor/power_bi/README.md)                       | Dashboard, lineage                       |
| [redshift](metaphor/redshift/README.md)                       | Schema, description, statistics, queries |
| [redshift.lineage](metaphor/redshift/lineage/README.md)       | Lineage                                  |
| [redshift.profile](metaphor/redshift/profile/README.md)       | Data profile                             |
| [snowflake](metaphor/snowflake/README.md)                     | Schema, description, statistics, queries |
| [snowflake.lineage](metaphor/snowflake/lineage/README.md)     | Lineage                                  |
| [snowflake.profile](metaphor/snowflake/profile/README.md)     | Data profile                             |
| [synapse](metaphor/synapse//README.md)                        | Schema, queries                          |
| [tableau](metaphor/tableau/README.md)                         | Dashboard, lineage                       |
| [thought_spot](metaphor/thought_spot/README.md)               | Dashboard, lineage                       |
| [unity_catalog](metaphor/unity_catalog/README.md)             | Schema, description                      |

## Development

See [Development Environment](docs/develop.md) for more instructions on how to setup your local development environment.

## Custom Connectors

See [Adding a Custom Connector](docs/custom.md) for instructions and a full example on how to create your custom connectors.
