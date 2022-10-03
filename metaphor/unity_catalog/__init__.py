from metaphor.common.cli import cli_main
from metaphor.unity_catalog.extractor import UnityCatalogExtractor


def main(config_file: str):
    cli_main(UnityCatalogExtractor, config_file)
