from metaphor.common.cli import cli_main
from metaphor.power_bi.extractor import PowerBIExtractor


def main(config_file: str):
    cli_main(PowerBIExtractor, config_file)
