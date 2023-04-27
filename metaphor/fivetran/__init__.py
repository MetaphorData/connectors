from metaphor.common.cli import cli_main
from metaphor.fivetran.extractor import FivetranExtractor


def main(config_file: str):
    cli_main(FivetranExtractor, config_file)
