from metaphor.common.cli import cli_main

from .config import RedshiftRunConfig
from .extractor import RedshiftExtractor

if __name__ == "__main__":
    cli_main("Redshift metadata extractor", RedshiftRunConfig, RedshiftExtractor)
