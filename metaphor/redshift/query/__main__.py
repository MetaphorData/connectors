from metaphor.common.cli import cli_main

from .extractor import RedshiftQueryExtractor

if __name__ == "__main__":
    cli_main("Redshift query history extractor", RedshiftQueryExtractor)
