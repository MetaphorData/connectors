from metaphor.common.cli import cli_main
from metaphor.hive.extractor import HiveExtractor


def main(config_file: str):
    cli_main(HiveExtractor, config_file)
