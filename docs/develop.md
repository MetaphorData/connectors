# Development Environment

## Install Python

Install [pyenv](https://github.com/pyenv/pyenv#installation) to manage multiple versions of Python on your system

```shell
brew install pyenv

For bash:
echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init -)"\nfi' >> ~/.bash_profile

For zsh:
echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init -)"\nfi' >> ~/.zshrc
```

Install Python version `3.7.10` if haven't done so already

```shell
pyenv install 3.7.10
```

Verify that it's the version selected by `pyenv` for the project.

```shell
python -V
Python 3.7.10
```

> Note: We recommend using the latest 3.7 release to ensure maximum compatibility. If you're developing on an Apple Silicon Macs, please develop in a [Rosetta Terminal](https://www.courier.com/blog/tips-and-tricks-to-setup-your-apple-m1-for-development).

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

Install [pre-commit](https://pre-commit.com/)
```
brew install pre-commit
```

Install git hook
```
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

We use [flakehell](https://github.com/life4/flakehell), which is a wrapper around the popular [flake8](https://github.com/PyCQA/flake8) linter. You can invoke the linter using the following command

```shell
flakehell lint
```
