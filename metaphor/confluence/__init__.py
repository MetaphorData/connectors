from metaphor.common.cli import cli_main
from metaphor.confluence.extractor import ConfluenceExtractor


def main(config_file: str):
    cli_main(ConfluenceExtractor, config_file)
