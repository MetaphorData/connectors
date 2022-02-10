from metaphor.common.cli import cli_main

from .extractor import RedshiftLineageExtractor

if __name__ == "__main__":
    cli_main("Redshift lineage metadata extractor", RedshiftLineageExtractor)
