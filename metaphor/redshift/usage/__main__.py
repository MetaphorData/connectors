from metaphor.common.cli import cli_main

from .config import RedshiftUsageRunConfig
from .extractor import RedshiftUsageExtractor

if __name__ == "__main__":
    cli_main(
        "Redshift usage metadata extractor",
        RedshiftUsageRunConfig,
        RedshiftUsageExtractor,
    )
