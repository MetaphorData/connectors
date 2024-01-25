from metaphor.alation.extractor import AlationExtractor
from metaphor.common.cli import cli_main


def main(config_file: str):
    cli_main(AlationExtractor, config_file)
