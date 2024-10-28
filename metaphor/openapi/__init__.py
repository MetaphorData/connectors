from metaphor.common.cli import cli_main
from metaphor.openapi.extractor import OpenAPIExtractor


def main(config_file: str):
    cli_main(OpenAPIExtractor, config_file)
