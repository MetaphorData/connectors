# Development Environment

## Install Python

We recommend using [pyenv](https://github.com/pyenv/pyenv) to manage multiple versions of Python on your system. Refer to the [installation guide](https://github.com/pyenv/pyenv#installation) for your system.

Install Python `3.8` if haven't done so already

```shell
pyenv install 3.8
```

Verify that it's the version selected by `pyenv` for the project.

```shell
python -V
Python 3.8
```

If you prefer to use Python 3.8 only for this project, you can run the following command to generate a `.python_version` file.

```shell
pyenv local 3.8
```

> Note: We recommend using the latest 3.8 release to ensure maximum compatibility. However, if you're developing on an Apple Silicon Mac, please either run in a [Rosetta Terminal](https://www.courier.com/blog/tips-and-tricks-to-setup-your-apple-m1-for-development) or use the closest version that natively supports Apple Silicon (3.8.10 as of now).

## (macOS only) Install External Dependencies

Some Python packages require external dependencies to build on macOS, e.g. [pymssql](https://github.com/pymssql/pymssql). You can use [homebrew](https://brew.sh/) to install these:

```shell
brew install freetds
```

## Install Poetry

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

To make life easier, we recommend to always develop in a poetry shell. Many of the following commands will only work inside a poetry shell as well.

## Setup Pre-commit

Install [pre-commit](https://pre-commit.com/#installation) then setup git hooks

```shell
pre-commit install
```

## Run Extractors

Each extractor module can be directly invoked from command line during development. The extractor takes a single `config_file` argument, e.g.

```shell
python -m metaphor.postgresql config/postgres.json
```

## Code Styling

### Type Checker

We use [mypy](http://mypy-lang.org/) to type-check Python code.

Pre-commit hook is setup to automatically type-check all Python files before committing. You can manually invoke it inside poetry shell,

```shell
mypy .
```

### Formatter

We use [black](https://github.com/psf/black) && [isort](https://pycqa.github.io/isort/) to format Python code in an opinionated way.

Pre-commit hook is setup to automatically format all Python files before committing.

### Linter

We use [flake8](https://github.com/PyCQA/flake8) linter to check the style and quality of some python code. You can invoke the linter using the following command

```shell
flake8 .
```

## Publishing

[The PyPI package](https://pypi.org/project/metaphor-connectors/) and [docker image](https://hub.docker.com/r/metaphordata/connectors) are automatically built and published as part of the [CD workflow](../.github/workflows/cd.yml). Please make sure to bump up the version in [pyproject.toml](../pyproject.toml) along with the PR to trigger the publishing workflow.
