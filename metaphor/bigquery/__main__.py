from metaphor.common.cli import cli_main

from .extractor import BigQueryExtractor, BigQueryRunConfig

if __name__ == "__main__":
    cli_main("BigQuery metadata extractor", BigQueryRunConfig, BigQueryExtractor)
