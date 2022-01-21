from metaphor.common.cli import cli_main

from .extractor import RedshiftExtractor

if __name__ == "__main__":
    cli_main("Redshift metadata extractor", RedshiftExtractor)
