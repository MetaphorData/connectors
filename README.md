# Metaphor Connectors

This repository contains a collection of Python-based "connectors" that extract metadata from various sources to ingest into the Metaphor app.

Each connector is placed under its own directory under `metaphor` and is expected to extend `metaphor.common.BaseExtractor`.

## Installation

[This package](https://pypi.org/project/metaphor-connectors/) is automatically published to [PyPI](https://pypi.org/) as part of the [CI/CD workflow](.github/workflows/cicd.yml). You can install it in your environment using pip.

```
pip install metaphor-connectors[all]
```

This will install all the connectors and required dependencies. You can also choose to install only a subset of the dependencies by installing specific [extra](https://packaging.python.org/tutorials/installing-packages/#installing-setuptools-extras), e.g.

```
pip install metaphor-connectors[snowflake]
```

Similary, you can also install the package using `requirements.txt` or `pyproject.toml`.

## Development

See [Development Environment](docs/develop.md) for more instructions on how to setup your local development environment.
