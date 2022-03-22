from metaphor.common.cli import cli_main

from .extractor import BigQueryQueryExtractor

if __name__ == "__main__":
    cli_main("BigQuery query history extractor", BigQueryQueryExtractor)
