from metaphor.common.cli import cli_main
from metaphor.thought_spot.extractor import ThoughtspotExtractor


def main(config_file: str):
    cli_main(ThoughtspotExtractor, config_file)
