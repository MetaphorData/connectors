from metaphor.common.cli import cli_main
from metaphor.manual.metadata.extractor import CustomMetadataExtractor


def main(config_file: str):
    cli_main(CustomMetadataExtractor, config_file)
