from metaphor.common.cli import cli_main
from metaphor.sharepoint.extractor import SharepointExtractor


def main(config_file: str):
    cli_main(SharepointExtractor, config_file)
