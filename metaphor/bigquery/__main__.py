from metaphor.common.cli import cli_main

from .extractor import BigQueryExtractor

if __name__ == "__main__":
    cli_main("BigQuery metadata extractor", BigQueryExtractor)
