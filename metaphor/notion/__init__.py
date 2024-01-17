from metaphor.common.cli import cli_main
from metaphor.notion.extractor import NotionExtractor


def main(config_file: str):
    cli_main(NotionExtractor, config_file)
