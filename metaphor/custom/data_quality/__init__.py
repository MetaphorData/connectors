from metaphor.common.cli import cli_main
from metaphor.custom.data_quality.extractor import CustomDataQualityExtractor


def main(config_file: str):
    cli_main(CustomDataQualityExtractor, config_file)
