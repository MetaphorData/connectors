from metaphor.common.cli import cli_main

from .extractor import BigQueryProfileExtractor

if __name__ == "__main__":
    cli_main(
        "BigQuery data profile extractor",
        BigQueryProfileExtractor,
    )
