from metaphor.common.cli import cli_main
from metaphor.great_expectations.extractor import GreatExpectationsExtractor


def main(config_file: str):
    cli_main(GreatExpectationsExtractor, config_file)
