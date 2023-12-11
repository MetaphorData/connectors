from metaphor.common.cli import cli_main
from metaphor.trino.extractor import TrinoExtractor


def main(config_file: str):
    cli_main(TrinoExtractor, config_file)
