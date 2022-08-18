from metaphor.common.cli import cli_main
from metaphor.thought_spot.extractor import ThoughtSpotExtractor


def main(config_file: str):
    cli_main(ThoughtSpotExtractor, config_file)
