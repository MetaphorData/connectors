from metaphor.common.cli import cli_main
from metaphor.glue.extractor import GlueExtractor


def main(config_file: str):
    cli_main(GlueExtractor, config_file)
