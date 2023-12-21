from metaphor.common.cli import cli_main
from metaphor.datahub.extractor import DatahubExtractor


def main(config_file: str):
    cli_main(DatahubExtractor, config_file)
