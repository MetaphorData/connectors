from metaphor.common.cli import cli_main
from metaphor.custom.lineage.extractor import CustomLineageExtractor


def main(config_file: str):
    cli_main(CustomLineageExtractor, config_file)
