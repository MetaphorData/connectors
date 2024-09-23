from metaphor.common.cli import cli_main
from metaphor.quick_sight.extractor import QuickSightExtractor


def main(config_file: str):
    cli_main(QuickSightExtractor, config_file)
