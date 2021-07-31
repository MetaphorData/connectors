from metaphor.common.cli import cli_main

from .extractor import DbtExtractor, DbtRunConfig

if __name__ == "__main__":
    cli_main("DBT metadata extractor", DbtRunConfig, DbtExtractor)
