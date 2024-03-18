from metaphor.common.cli import cli_main
from metaphor.monday.extractor import MondayExtractor


def main(config_file: str):
    cli_main(MondayExtractor, config_file)
