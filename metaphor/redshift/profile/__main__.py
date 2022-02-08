from metaphor.common.cli import cli_main

from .extractor import RedshiftProfileExtractor

if __name__ == "__main__":
    cli_main("Redshift data profile extractor", RedshiftProfileExtractor)
