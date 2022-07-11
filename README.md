<img src="./logo.png" width="300" />

# Metaphor Connectors

[![CI](https://github.com/MetaphorData/connectors/actions/workflows/ci.yml/badge.svg)](https://github.com/MetaphorData/connectors/actions/workflows/ci.yml)
[![CodeQL](https://github.com/MetaphorData/connectors/workflows/CodeQL/badge.svg)](https://github.com/MetaphorData/connectors/actions/workflows/codeql-analysis.yml)
[![PyPI Version](https://img.shields.io/pypi/v/metaphor-connectors)](https://pypi.org/project/metaphor-connectors/)
![Python version 3.7+](https://img.shields.io/badge/python-3.7%2B-blue)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/MetaphorData/connectors.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/MetaphorData/connectors/context:python)
[![License](https://img.shields.io/github/license/MetaphorData/connectors)](https://github.com/MetaphorData/connectors/blob/master/LICENSE)

This repository contains a collection of Python-based "connectors" that extract metadata from various sources to ingest into the Metaphor app.

## Installation

This package requires Python 3.7+ installed. You can verify the version on your system by running the following command,

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

## Connectors

Each connector is placed under its own directory under [metaphor](./metaphor) and extends the `metaphor.common.BaseExtractor` class.

| Connector                                                             | Metadata                                 |
|-----------------------------------------------------------------------|------------------------------------------|  
| [metaphor.airflow_plugin](metaphor/airflow_plugin/README.md)          | Lineage                                  |
| [metaphor.bigquery](metaphor/bigquery/README.md)                      | Schema, description, statistics          |
| [metaphor.bigquery.lineage](metaphor/bigquery/lineage/README.md)      | Lineage                                  |
| [metaphor.bigquery.profile](metaphor/bigquery/profile/README.md)      | Data profile                             |
| [metaphor.bigquery.query](metaphor/bigquery/query/README.md)          | Queries                                  |
| [metaphor.bigquery.usage](metaphor/bigquery/usage/README.md)          | Data usage                               |
| [metaphor.dbt](metaphor/dbt/README.md)                                | dbt model, test, lineage                 |
| [metaphor.dbt.cloud](metaphor/dbt/cloud/README.md)                    | dbt model, test, lineage                 |
| [metaphor.google_directory](metaphor/google_directory/README.md)      | User profile                             |
| [metaphor.looker](metaphor/looker/README.md)                          | Looker view, explore, dashboard, lineage |
| [metaphor.manual.lineage](metaphor/manual/lineage/README.md)          | Lineage                                  |
| [metaphor.metabase](metaphor/metabase/README.md)                      | Dashboard, lineage                       |
| [metaphor.postgresql](metaphor/postgresql/README.md)                  | Schema, description, statistics          |
| [metaphor.postgresql.profile](metaphor/postgresql/profile/README.md)  | Data profile                             |
| [metaphor.postgresql.usage](metaphor/postgresql/usage/README.md)      | Usage                                    |
| [metaphor.power_bi](metaphor/power_bi/README.md)                      | Dashboard, lineage                       |
| [metaphor.redshift](metaphor/redshift/README.md)                      | Schema, description, statistics          |
| [metaphor.redshift.lineage](metaphor/redshift/lineage/README.md)      | Lineage                                  |
| [metaphor.redshift.profile](metaphor/redshift/profile/README.md)      | Data profile                             |
| [metaphor.redshift.query](metaphor/redshift/query/README.md)          | Queries                                  |
| [metaphor.redshift.usage](metaphor/redshift/usage/README.md)          | Usage                                    |
| [metaphor.snowflake](metaphor/snowflake/README.md)                    | Schema, description, statistics          |
| [metaphor.snowflake.lineage](metaphor/snowflake/lineage/README.md)    | Lineage                                  |
| [metaphor.snowflake.profile](metaphor/snowflake/profile/README.md)    | Data profile                             |
| [metaphor.snowflake.query](metaphor/snowflake/query/README.md)        | Queries                                  |
| [metaphor.snowflake.usage](metaphor/snowflake/usage/README.md)        | Data usage                               |
| [metaphor.tableau](metaphor/tableau/README.md)                        | Dashboard, lineage                       |

## Development

See [Development Environment](docs/develop.md) for more instructions on how to setup your local development environment.
