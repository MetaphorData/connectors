# Metaphor Connectors

This repository contains a collection of Python-based "connectors" that extract metadata from various sources to ingest into the Metaphor app.

Each connector is placed under its own directory under `metaphor` and is expected to extend `metaphor.common.BaseExtractor`.

## Environment Setup

### Install Python

Install [pyenv](https://github.com/pyenv/pyenv#installation) to manage multiple versions of Python on your system

```shell
brew install pyenv

For bash:
echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init -)"\nfi' >> ~/.bash_profile

For zsh:
echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init -)"\nfi' >> ~/.zshrc
```

Install Python version `miniforge3-4.9.2` if haven't done so already

```shell
pyenv install miniforge3-4.9.2
```

Note: The reason for installing miniforge3 instead of a vanilla python version is due to some library compatibility issue on macOS with Apple M1 chip, such as `pyarrow`. Once those issues are resolved, we can revert back to the vanilla Python.

Verify that it's the version selected by `pyenv` for the project, as specified in `.python-version`.

```shell
python -V
Python 3.9.1 (or 3.8.7 on Intel based Mac)
```

### Extra Installation for M1

First install pyarrow using conda, this is also due to compatibility on M1, currently only the pyarrow on conda-forge is compatible.

```shell
conda install -c conda-forge pyarrow
```

### Install Poetry

Install [poetry](https://python-poetry.org/) package manager

```shell
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
```

Use `poetry` to setup virtual env

```shell
poetry install -E all
```

Activate virtual env during development

```shell
poetry shell
```

To deactivate the virtual env, simply run

```shell
exit
```

### Setup Pre-commit

Install [pre-commit](https://pre-commit.com/)
```
brew install pre-commit
```

Install git hook
```
pre-commit install
```

## Run Extractors

Each extractor module can be directly invoked from command line, inside poetry shell. The extractor takes a single `config_file` argument, e.g.

```shell
poetry shell
python -m metaphor.postgresql config/postgres.json
```

## Code Styling

### Type Checker

We use [mypy](http://mypy-lang.org/) to type-check Python code.

Pre-commit hook is setup to automatically type-check all Python files before committing. You can manually invoke it inside poetry shell, e.g.
```shell
poetry shell
mypy .
```

### Formatter

We use [black](https://github.com/psf/black) && [isort](https://pycqa.github.io/isort/) to format Python code in an opinionated way.

Pre-commit hook is setup to automatically format all Python files before committing.

Follow [this tutorial](https://dev.to/adamlombard/how-to-use-the-black-python-code-formatter-in-vscode-3lo0) to setup auto formatting in VS Code.

### Linter

We use [flakehell](https://github.com/life4/flakehell), which is a wrapper around the popular [flake8](https://github.com/PyCQA/flake8) linter. You can invoke the linter using the following command

```shell
poetry shell
python flakehell
```
