from metaphor.common.cli import cli_main
from metaphor.informatica.extractor import InformaticaExtractor


def main(config_file: str):
    cli_main(InformaticaExtractor, config_file)
