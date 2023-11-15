from metaphor.common.cli import cli_main
from metaphor.unity_catalog.profile.extractor import UnityCatalogProfileExtractor


def main(config_file: str):
    cli_main(UnityCatalogProfileExtractor, config_file)
