from metaphor.common.cli import cli_main
from metaphor.tableau.extractor import TableauExtractor


def main(config_file: str):
    cli_main(TableauExtractor, config_file)
