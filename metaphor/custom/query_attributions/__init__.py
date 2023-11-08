from metaphor.common.cli import cli_main
from metaphor.custom.query_attributions.extractor import (
    CustomQueryAttributionsExtractor,
)


def main(config_file: str):
    cli_main(CustomQueryAttributionsExtractor, config_file)
