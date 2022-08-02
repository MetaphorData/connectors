from metaphor.common.cli import cli_main
from metaphor.looker.extractor import LookerExtractor


def main(config_file: str):
    cli_main(LookerExtractor, config_file)
