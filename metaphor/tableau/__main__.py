from metaphor.common.cli import cli_main

from .extractor import TableauExtractor

if __name__ == "__main__":
    cli_main("Tableau metadata extractor", TableauExtractor)
