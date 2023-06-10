from metaphor.common.cli import cli_main
from metaphor.custom.metadata.extractor import CustomMetadataExtractor


def main(config_file: str):
    cli_main(CustomMetadataExtractor, config_file)
