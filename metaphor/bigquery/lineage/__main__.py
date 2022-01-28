from metaphor.common.cli import cli_main

from .extractor import BigQueryLineageExtractor

if __name__ == "__main__":
    cli_main("BigQuery lineage metadata extractor", BigQueryLineageExtractor)
