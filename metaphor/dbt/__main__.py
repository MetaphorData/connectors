from metaphor.common.cli import cli_main

from .extractor import DbtExtractor

if __name__ == "__main__":
    cli_main("DBT metadata extractor", DbtExtractor)
