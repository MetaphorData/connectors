from metaphor.athena.extractor import AthenaExtractor
from metaphor.common.cli import cli_main


def main(config_file: str):
    cli_main(AthenaExtractor, config_file)
