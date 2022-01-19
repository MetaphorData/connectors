from metaphor.common.cli import cli_main

from .config import BigQueryProfileRunConfig
from .extractor import BigQueryProfileExtractor

if __name__ == "__main__":
    cli_main(
        "BigQuery usage metadata extractor",
        BigQueryProfileRunConfig,
        BigQueryProfileExtractor,
    )
