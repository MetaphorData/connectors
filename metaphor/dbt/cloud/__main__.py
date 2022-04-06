from metaphor.common.cli import cli_main

from .extractor import DbtCloudExtractor

if __name__ == "__main__":
    cli_main("DBT cloud metadata extractor", DbtCloudExtractor)
