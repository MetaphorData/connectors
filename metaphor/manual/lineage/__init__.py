from metaphor.common.cli import cli_main
from metaphor.manual.lineage.extractor import ManualLineageExtractor


def main(config_file: str):
    cli_main(ManualLineageExtractor, config_file)
