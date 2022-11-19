from metaphor.common.cli import cli_main
from metaphor.manual.data_quality.extractor import ManualDataQualityExtractor


def main(config_file: str):
    cli_main(ManualDataQualityExtractor, config_file)
