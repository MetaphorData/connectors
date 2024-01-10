from metaphor.common.cli import cli_main
from metaphor.static_web.extractor import StaticWebExtractor


def main(config_file: str):
    cli_main(StaticWebExtractor, config_file)
