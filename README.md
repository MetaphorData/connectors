<img src="./logo.png" width="300" />

# Metaphor Connectors

[![CI/CD](https://github.com/MetaphorData/connectors/actions/workflows/cicd.yml/badge.svg)](https://github.com/MetaphorData/connectors/actions/workflows/cicd.yml)
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

| Connector                                                          | Metadata                           |
|--------------------------------------------------------------------|------------------------------------|  
| [metaphor.bigquery](metaphor/bigquery/README.md)                   | Schema, Description                |
| [metaphor.bigquery.profile](metaphor/bigquery/profile/README.md)   | Data Profile                       |
| [metaphor.bigquery.usage](metaphor/bigquery/usage/README.md)       | Usage                              |
| [metaphor.dbt](metaphor/dbt/README.md)                             | dbt models, tests, lineage         |
| [metaphor.google_directory](metaphor/google_directory/README.md)   | User                               |
| [metaphor.looker](metaphor/looker/README.md)                       | Looker views, explores, dashboards |
| [metaphor.postgresql](metaphor/postgresql/README.md)               | Schema, Description, Statistics    |
| [metaphor.redshift](metaphor/redshift/README.md)                   | Schema, Description, Statistics    |
| [metaphor.snowflake](metaphor/snowflake/README.md)                 | Schema, Description, Statistics    |
| [metaphor.snowflake.profile](metaphor/snowflake/profile/README.md) | Data Profile                       |
| [metaphor.snowflake.usage](metaphor/snowflake/usage/README.md)     | Usage                              |
| [metaphor.tableau](metaphor/tableau/README.md)                     | Dashboard                          |

## Development

See [Development Environment](docs/develop.md) for more instructions on how to setup your local development environment.
