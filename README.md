# Metaphor Connectors

[![CI/CD](https://github.com/MetaphorData/connectors/actions/workflows/cicd.yml/badge.svg)](https://github.com/MetaphorData/connectors/actions/workflows/cicd.yml)
[![PyPI Version](https://img.shields.io/pypi/v/metaphor-connectors)](https://pypi.org/project/metaphor-connectors/)
![Python version 3.7+](https://img.shields.io/badge/python-3.7%2B-blue)
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

Similary, you can also install the package using `requirements.txt` or `pyproject.toml`.

## Connectors

Each connector is placed under its own directory under [metaphor](./metaphor) and extends the `metaphor.common.BaseExtractor` class.

| Connector | Metadata  |
| --------- | --------- |  
| [metaphor.bigquery](metaphor/bigquery/README.md) | Schema, Description |
| [metaphor.dbt](metaphor/dbt/README.md) | dbt definitions, Lineage |
| [metaphor.google_directory](metaphor/google_directory/README.md) | User |
| [metaphor.snowflake](metaphor/snowflake/README.md) | Schema, Description |
| [metaphor.snowflake.usage](metaphor/snowflake/usage/README.md) | Usage |
| [metaphor.snowflake.profiling](metaphor/snowflake/profiling/README.md) | Data Profile |

## Development

See [Development Environment](docs/develop.md) for more instructions on how to setup your local development environment.
