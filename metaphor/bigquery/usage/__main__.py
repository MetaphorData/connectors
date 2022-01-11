from metaphor.common.cli import cli_main

from .extractor import BigQueryUsageExtractor, BigQueryUsageRunConfig

if __name__ == "__main__":
    cli_main(
        "BigQuery usage metadata extractor",
        BigQueryUsageRunConfig,
        BigQueryUsageExtractor,
    )
